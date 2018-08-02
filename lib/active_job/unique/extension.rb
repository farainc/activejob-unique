require 'active_support/concern'
require 'active_job/base'

module ActiveJob
  module Unique
    module Extension
      extend ActiveSupport::Concern
      JOB_PROGRESS_ENQUEUE_ATTEMPTED = :enqueue_attempted
      JOB_PROGRESS_ENQUEUE_PROCESSING = :enqueue_processing
      JOB_PROGRESS_ENQUEUE_FAILED = :enqueue_failed
      JOB_PROGRESS_ENQUEUE_PROCESSED = :enqueue_processed
      JOB_PROGRESS_ENQUEUE_SKIPPED = :enqueue_skipped

      JOB_PROGRESS_PERFORM_ATTEMPTED = :perform_attempted
      JOB_PROGRESS_PERFORM_PROCESSING = :perform_processing
      JOB_PROGRESS_PERFORM_FAILED = :perform_failed
      JOB_PROGRESS_PERFORM_PROCESSED = :perform_processed
      JOB_PROGRESS_PERFORM_SKIPPED = :perform_skipped

      UNIQUENESS_MODE_WHILE_EXECUTING = :while_executing
      UNIQUENESS_MODE_UNTIL_TIMEOUT = :until_timeout
      UNIQUENESS_MODE_UNTIL_EXECUTING = :until_executing
      UNIQUENESS_MODE_UNTIL_AND_WHILE_EXECUTING = :until_and_while_executing

      included do
        class_attribute :uniqueness_mode
        class_attribute :uniqueness_duration
        class_attribute :uniqueness_expiration
        class_attribute :debug_mode

        attr_accessor :provider_job_id, :priority, :executions #compatible with rails 4.x
        attr_accessor :unique_as_skipped, :uniqueness_id, :job_progress, :skip_reason

        before_enqueue do |job|
          @uniqueness_id = Digest::MD5.hexdigest([job.queue_name, job.class.name, job.arguments].inspect.to_s)
        end

        around_enqueue do |job, block|
          r = nil

          @job_progress = JOB_PROGRESS_ENQUEUE_ATTEMPTED
          incr_job_stats(job)

          # must be keep this block
          if allow_enqueue_uniqueness?(job)
            @job_progress = JOB_PROGRESS_ENQUEUE_PROCESSING
            incr_job_stats(job)

            write_uniqueness_before_enqueue(job)

            begin
              r = block.call

              @job_progress = JOB_PROGRESS_ENQUEUE_PROCESSED
              incr_job_stats(job)

              write_uniqueness_after_enqueue(job)
            rescue StandardError => e
              @job_progress = JOB_PROGRESS_ENQUEUE_FAILED
              incr_job_stats(job)

              update_uniqueness_progress(job)
              raise e
            end
          else
            @job_progress = JOB_PROGRESS_ENQUEUE_SKIPPED
            incr_job_stats(job)
            update_uniqueness_progress(job)
          end

          r
        end

        around_perform do |job, block|
          r = nil

          @job_progress = JOB_PROGRESS_PERFORM_ATTEMPTED
          incr_job_stats(job)
          update_uniqueness_progress(job)

          # must be keep this block
          if allow_perform_uniqueness?(job)
            @job_progress = JOB_PROGRESS_PERFORM_PROCESSING
            incr_job_stats(job)

            write_uniqueness_before_perform(job)

            # double verify perform_processing status and job_id is the same
            if ensure_job_progress_perform_processing?(job)
              begin
                r = block.call

                @job_progress = JOB_PROGRESS_PERFORM_PROCESSED
                incr_job_stats(job)

                write_uniqueness_after_perform(job)
              rescue StandardError => e
                @job_progress = JOB_PROGRESS_PERFORM_FAILED
                incr_job_stats(job)

                update_uniqueness_progress(job)
                raise e
              end
            else
              @job_progress = JOB_PROGRESS_PERFORM_SKIPPED
              incr_job_stats(job)
              update_uniqueness_progress(job)
            end
          else
            @job_progress = JOB_PROGRESS_PERFORM_SKIPPED
            incr_job_stats(job)
            update_uniqueness_progress(job)
          end

          r
        end
      end

      def enqueue_processing?
        job_progress == JOB_PROGRESS_ENQUEUE_PROCESSING
      end

      def enqueue_processed?
        job_progress == JOB_PROGRESS_ENQUEUE_PROCESSED
      end

      def enqueue_skipped?
        job_progress == JOB_PROGRESS_ENQUEUE_SKIPPED
      end

      def perform_processing?
        job_progress == JOB_PROGRESS_PERFORM_PROCESSING
      end

      def perform_processed?
        job_progress == JOB_PROGRESS_PERFORM_PROCESSED
      end

      def perform_skipped?
        job_progress == JOB_PROGRESS_PERFORM_SKIPPED
      end

      def stats_adapter
        self.class.queue_adapter
      end

      # uniqueness job
      def invalid_uniqueness_mode?
        ![UNIQUENESS_MODE_WHILE_EXECUTING,
          UNIQUENESS_MODE_UNTIL_EXECUTING,
          UNIQUENESS_MODE_UNTIL_AND_WHILE_EXECUTING,
          UNIQUENESS_MODE_UNTIL_TIMEOUT].include?(self.class.uniqueness_mode)
      end

      def enqueue_only_uniqueness_mode?
        UNIQUENESS_MODE_UNTIL_EXECUTING == self.class.uniqueness_mode
      end

      def perform_only_uniqueness_mode?
        UNIQUENESS_MODE_WHILE_EXECUTING == self.class.uniqueness_mode
      end

      def until_timeout_uniqueness_mode?
        UNIQUENESS_MODE_UNTIL_TIMEOUT == self.class.uniqueness_mode
      end

      def enqueue_stage?(progress)
        [JOB_PROGRESS_ENQUEUE_ATTEMPTED,
         JOB_PROGRESS_ENQUEUE_PROCESSING,
         JOB_PROGRESS_ENQUEUE_PROCESSED,
         JOB_PROGRESS_ENQUEUE_FAILED,
         JOB_PROGRESS_ENQUEUE_SKIPPED].include?(progress.to_s.to_sym)
      end

      def perform_stage?(progress)
        [JOB_PROGRESS_PERFORM_ATTEMPTED,
         JOB_PROGRESS_PERFORM_PROCESSING,
         JOB_PROGRESS_PERFORM_PROCESSED,
         JOB_PROGRESS_PERFORM_FAILED,
         JOB_PROGRESS_PERFORM_SKIPPED].include?(progress.to_s.to_sym)
      end

      def duplicated_job_in_queue?(job)
        return false unless stats_adapter.respond_to?(:duplicated_job_in_queue?)

        stats_adapter.duplicated_job_in_queue?(uniqueness_id, job.queue_name)
      end

      def duplicated_job_in_worker?(job)
        return false unless stats_adapter.respond_to?(:duplicated_job_in_worker?)

        stats_adapter.duplicated_job_in_worker?(uniqueness_id, job)
      end

      def enqueue_stage_job?(job)
        return false unless stats_adapter.respond_to?(:enqueue_stage_job?)

        stats_adapter.enqueue_stage_job?(uniqueness_id, job.queue_name)
      end

      def perform_stage_job?(job)
        return false unless stats_adapter.respond_to?(:perform_stage_job?)

        stats_adapter.perform_stage_job?(uniqueness_id, job.queue_name)
      end

      def allow_enqueue_uniqueness?(job)
        return true if job.unique_as_skipped
        return true if invalid_uniqueness_mode?
        return true if perform_only_uniqueness_mode?

        # disallow duplicated_job_in_queue
        if duplicated_job_in_queue?(job)
          @skip_reason = 'enqueue:duplicated_job_in_queue'
          return false
        end

        # allow enqueue_only_uniqueness_mode if no duplicated_job_in_queue
        return true if enqueue_only_uniqueness_mode?

        # disallow duplicated_job_in_worker
        if duplicated_job_in_worker?(job)
          @skip_reason = 'enqueue:duplicated_job_in_worker'
          return false
        end

        # allow dirty_uniqueness
        uniqueness = load_uniqueness(job)
        return true if dirty_uniqueness?(uniqueness)

        progress = uniqueness['p'].to_s.to_sym

        # disallow perform_processing progress
        if progress == JOB_PROGRESS_PERFORM_PROCESSING
          @skip_reason = 'enqueue:perform_processing'
          return false
        end

        # disallow until_timeout_uniqueness_mode with perform_processed progress
        if until_timeout_uniqueness_mode? && progress == JOB_PROGRESS_PERFORM_PROCESSED
          @skip_reason = 'enqueue:until_timeout:perform_processed'
          return false
        end

        true
      end

      def allow_perform_uniqueness?(job)
        return true if job.unique_as_skipped
        return true if invalid_uniqueness_mode?
        return true if enqueue_only_uniqueness_mode?

        uniqueness = load_uniqueness(job)
        return true if dirty_uniqueness?(uniqueness)

        job_id = uniqueness['j']
        return true if job_id == job.job_id

        if duplicated_job_in_worker?(job)
          @skip_reason = 'perform:duplicated_job_in_worker'
          return false
        end

        return true if enqueue_stage?(uniqueness['p'])

        @skip_reason = 'perform:not_in_enqueue_stage'
        false
      end

      def ensure_job_progress_perform_processing?(job)
        return true if job.unique_as_skipped
        return true if invalid_uniqueness_mode?
        return true if enqueue_only_uniqueness_mode?
        return true unless stats_adapter.respond_to?(:write_uniqueness_progress)

        uniqueness = load_uniqueness(job)
        return true if uniqueness.blank?

        # ensure job_id is same as uniqueness
        if uniqueness['j'] != job.job_id
          @skip_reason = "perform:not_same_job_id (#{uniqueness['j']}/#{job.job_id})"
          return false
        end

        # ensure job status changed to perform_processing
        (0..9).each do |i|
          return true if uniqueness['p'] == JOB_PROGRESS_PERFORM_PROCESSING

          # wait 500ms if JOB_PROGRESS_PERFORM_ATTEMPTED
          if uniqueness['p'] == JOB_PROGRESS_PERFORM_ATTEMPTED
            sleep(0.05)

            uniqueness = load_uniqueness(job)

            if uniqueness.blank?
              @skip_reason = "perform:uniqueness_invalid"
              return false
            end
          else
            @skip_reason = "perform:not_perform_processing (#{uniqueness['p']}/#{JOB_PROGRESS_PERFORM_PROCESSING})"
            break
          end
        end

        false
      end

      def dirty_uniqueness?(uniqueness)
        return true unless stats_adapter.respond_to?(:dirty_uniqueness?)

        stats_adapter.dirty_uniqueness?(uniqueness)
      end

      def read_uniqueness(job)
        stats_adapter.read_uniqueness(uniqueness_id, job.queue_name) if stats_adapter.respond_to?(:read_uniqueness)
      end

      def load_uniqueness(job)
        JSON.load(read_uniqueness(job)) rescue nil
      end

      def write_uniqueness_before_enqueue(job)
        # do not update uniqueness for perform_only_uniqueness_mode
        # when job is in perform_stage
        return if perform_only_uniqueness_mode? && perform_stage_job?(job)

        write_uniqueness_progress(job)
      end

      def write_uniqueness_after_enqueue(job)
        # do not update uniqueness for perform_only_uniqueness_mode
        # when job is in perform_stage
        return if perform_only_uniqueness_mode? && perform_stage_job?(job)

        update_uniqueness_progress(job)
      end

      def write_uniqueness_before_perform(job)
        write_uniqueness_progress(job)
      end

      def write_uniqueness_after_perform(job)
        if until_timeout_uniqueness_mode?
          update_uniqueness_progress(job)
        else
          expire_uniqueness(job)
        end
      end

      def incr_job_stats(job)
        return unless stats_adapter.respond_to?(:incr_job_stats)

        stats_adapter.incr_job_stats(job.queue_name,
                                     job.class.name,
                                     job_progress)
      end

      def calculate_timeout(job)
        timeout = 0

        case job_progress
        when JOB_PROGRESS_ENQUEUE_PROCESSING
          timeout = self.class.uniqueness_expiration.from_now.utc.to_i
        when JOB_PROGRESS_PERFORM_PROCESSING
          timeout = self.class.uniqueness_duration.from_now.utc.to_i
        end

        timeout
      end

      def calculate_expires(job)
        expires = 0

        # reset expires when enqueue processing
        case job_progress
        when JOB_PROGRESS_ENQUEUE_PROCESSING
          expires = self.class.uniqueness_expiration.from_now.utc.to_i
          expires = (job.scheduled_at + self.class.uniqueness_expiration).to_i if job.scheduled_at.present?
        when JOB_PROGRESS_PERFORM_PROCESSING
          expires = self.class.uniqueness_expiration.from_now.utc.to_i
        end

        expires
      end

      def write_uniqueness_progress(job)
        return if invalid_uniqueness_mode?
        return unless stats_adapter.respond_to?(:write_uniqueness_progress)

        timeout = calculate_timeout(job)
        expires = calculate_expires(job)

        stats_adapter.write_uniqueness_progress(uniqueness_id,
                                                job.queue_name,
                                                job.class.name,
                                                job.arguments,
                                                job.job_id,
                                                self.class.uniqueness_mode,
                                                job_progress,
                                                timeout,
                                                expires,
                                                self.class.debug_mode)
      end

      def update_uniqueness_progress(job)
        return if invalid_uniqueness_mode?
        return unless stats_adapter.respond_to?(:update_uniqueness_progress)

        stats_adapter.update_uniqueness_progress(uniqueness_id,
                                                 job.queue_name,
                                                 job.job_id,
                                                 job_progress,
                                                 skip_reason,
                                                 self.class.debug_mode)
      end

      def expire_uniqueness(job)
        return unless stats_adapter.respond_to?(:expire_uniqueness)

        stats_adapter.expire_uniqueness(uniqueness_id, job.queue_name, job_progress)
      end

      # add your instance methods here
      def reenqueue
        self.class.perform_later(*arguments)
      end

      def serialize
        {
          'job_class'       => self.class.name,
          'job_id'          => job_id,
          'provider_job_id' => provider_job_id,
          'queue_name'      => queue_name,
          'priority'        => priority,
          'arguments'       => serialize_arguments(arguments),
          'executions'      => executions,
          'locale'          => I18n.locale.to_s,
          'uniqueness_id'   => uniqueness_id,
          'unique_as_skipped' => unique_as_skipped
        }
      end

      def deserialize(job_data)
        self.job_id                 = job_data['job_id']
        self.provider_job_id        = job_data['provider_job_id']
        self.queue_name             = job_data['queue_name']
        self.priority               = job_data['priority']
        self.serialized_arguments   = job_data['arguments']
        self.executions             = job_data['executions']
        self.locale                 = job_data['locale'] || I18n.locale.to_s
        self.uniqueness_id          = job_data['uniqueness_id']
        self.unique_as_skipped      = job_data['unique_as_skipped']
      end

      # add your static(class) methods here
      module ClassMethods
        def unique_for(option = nil, expiration = 60.minutes)
          # default duration for a job is 10.minutes after perform processing
          # set longer duration for long running jobs
          self.uniqueness_duration = 10.minutes
          self.uniqueness_expiration = expiration

          if option == true
            self.uniqueness_mode = UNIQUENESS_MODE_UNTIL_AND_WHILE_EXECUTING
          elsif option.is_a?(Integer)
            self.uniqueness_mode = UNIQUENESS_MODE_UNTIL_TIMEOUT
            self.uniqueness_duration = option if option.positive?
          else
            self.uniqueness_mode = option.to_sym
          end

          true
        end

        def unique_for_debug_mode
          self.debug_mode = true
        end

        def perform_later_forced(*args)
          job = job_or_instantiate(*args)
          job.unique_as_skipped = true
          job.enqueue
        end

        def deserialize(job_data)
          job = job_data["job_class"].constantize.new
          job.deserialize(job_data)

          job
        end
      end
    end
  end
end

# include the extension
ActiveJob::Base.send(:include, ActiveJob::Unique::Extension)
