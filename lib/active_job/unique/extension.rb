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

        attr_accessor :unique_as_skipped, :uniqueness_id, :job_progress

        before_enqueue do |job|
          @uniqueness_id = Digest::MD5.hexdigest([job.queue_name, job.class.name, job.arguments].inspect.to_s)
        end

        around_enqueue do |job, block|
          r = nil

          @job_progress = JOB_PROGRESS_ENQUEUE_ATTEMPTED
          incr_job_stats(job)
          write_uniqueness_progress_and_addition(job)

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

              clean_uniqueness(job)
              raise e
            end
          else
            @job_progress = JOB_PROGRESS_ENQUEUE_SKIPPED
            incr_job_stats(job)
            write_uniqueness_progress_and_addition(job)
          end

          r
        end

        around_perform do |job, block|
          r = nil

          @job_progress = JOB_PROGRESS_PERFORM_ATTEMPTED
          incr_job_stats(job)
          write_uniqueness_progress_and_addition(job)

          # must be keep this block
          if allow_perform_uniqueness?(job)
            @job_progress = JOB_PROGRESS_PERFORM_PROCESSING
            incr_job_stats(job)

            write_uniqueness_before_perform(job)

            begin
              r = block.call

              @job_progress = JOB_PROGRESS_PERFORM_PROCESSED
              incr_job_stats(job)

              write_uniqueness_after_perform(job)
            rescue StandardError => e
              @job_progress = JOB_PROGRESS_PERFORM_FAILED
              incr_job_stats(job)

              clean_uniqueness(job)
              raise e
            end
          else
            @job_progress = JOB_PROGRESS_PERFORM_SKIPPED
            incr_job_stats(job)
            write_uniqueness_progress_and_addition(job)
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
        return false if duplicated_job_in_queue?(job)

        # allow enqueue_only_uniqueness_mode if no duplicated_job_in_queue
        return true if enqueue_only_uniqueness_mode?

        # disallow duplicated_job_in_worker
        return false if duplicated_job_in_worker?(job)

        # allow dirty_uniqueness
        uniqueness = load_uniqueness(job)
        return true if dirty_uniqueness?(uniqueness)

        progress = uniqueness['p'].to_s.to_sym

        # disallow perform_processing progress
        return false if progress == JOB_PROGRESS_PERFORM_PROCESSING

        # disallow until_timeout_uniqueness_mode with perform_processed progress
        return false if until_timeout_uniqueness_mode? && progress == JOB_PROGRESS_PERFORM_PROCESSED

        true
      end

      def allow_perform_uniqueness?(job)
        return true if job.unique_as_skipped
        return true if invalid_uniqueness_mode?
        return true if enqueue_only_uniqueness_mode?

        uniqueness = load_uniqueness(job)
        return true if dirty_uniqueness?(uniqueness)

        job_id = uniqueness['j']
        return true if job_id == job.provider_job_id
        return false if duplicated_job_in_worker?(job)

        progress = uniqueness['p'].to_s.to_sym
        addition = uniqueness['s'].to_s.to_sym

        progress == JOB_PROGRESS_ENQUEUE_PROCESSED && addition == JOB_PROGRESS_PERFORM_ATTEMPTED
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
        return if invalid_uniqueness_mode?

        # do not update uniqueness for perform_only_uniqueness_mode
        # when job is in perform_stage
        return if perform_only_uniqueness_mode? && perform_stage_job?(job)

        write_uniqueness_progress(job)
      end

      def write_uniqueness_after_enqueue(job)
        return if invalid_uniqueness_mode?

        # do not update uniqueness for perform_only_uniqueness_mode
        # when job is in perform_stage
        return if perform_only_uniqueness_mode? && perform_stage_job?(job)

        write_uniqueness_progress(job)
      end

      def write_uniqueness_before_perform(job)
        return if invalid_uniqueness_mode?

        write_uniqueness_progress(job)
      end

      def write_uniqueness_after_perform(job)
        should_clean_it = true
        should_clean_it = !write_uniqueness_progress_and_dump(job) if until_timeout_uniqueness_mode?

        clean_uniqueness(job) if should_clean_it
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
        when JOB_PROGRESS_ENQUEUE_PROCESSING, JOB_PROGRESS_ENQUEUE_PROCESSED
          timeout = self.class.uniqueness_expiration.from_now.utc.to_i
        when JOB_PROGRESS_PERFORM_PROCESSING
          timeout = self.class.uniqueness_duration.from_now.utc.to_i
        when JOB_PROGRESS_PERFORM_PROCESSED
          uniqueness = read_uniqueness(job)
          j = JSON.load(uniqueness) rescue nil
          timeout = j['t'].to_i if j.present?

          timeout = self.class.uniqueness_duration.from_now.utc.to_i unless timeout.positive?
        end

        timeout
      end

      def calculate_expires(job)
        expires = 0

        # reset expires when enqueue processing
        case job_progress
        when JOB_PROGRESS_ENQUEUE_PROCESSING, JOB_PROGRESS_ENQUEUE_PROCESSED
          expires = self.class.uniqueness_expiration.from_now.utc.to_i
          expires = (job.scheduled_at + self.class.uniqueness_expiration).to_i if job.scheduled_at.present?
        when JOB_PROGRESS_PERFORM_PROCESSING
          expires = self.class.uniqueness_expiration.from_now.utc.to_i
        else
          # always use saved expiration first
          uniqueness = read_uniqueness(job)
          j = JSON.load(uniqueness) rescue nil
          expires = j['e'].to_i if j.present?

          expires = self.class.uniqueness_expiration.from_now.utc.to_i unless expires.positive?
        end

        expires
      end

      def write_uniqueness_progress(job)
        return unless stats_adapter.respond_to?(:write_uniqueness_progress_and_dump)

        timeout = calculate_timeout(job)
        expires = calculate_expires(job)

        stats_adapter.write_uniqueness_progress_and_dump(uniqueness_id,
                                                         job.queue_name,
                                                         job.class.name,
                                                         job.arguments,
                                                         job.provider_job_id,
                                                         self.class.uniqueness_mode,
                                                         job_progress,
                                                         timeout,
                                                         expires)
      end

      def write_uniqueness_progress_and_dump(job)
        return false unless stats_adapter.respond_to?(:write_uniqueness_progress_and_dump)

        timeout = calculate_timeout(job)
        return false if timeout < Time.now.utc.to_i

        expires = calculate_expires(job)
        return false if expires < Time.now.utc.to_i

        stats_adapter.write_uniqueness_progress_and_dump(uniqueness_id,
                                                         job.queue_name,
                                                         job.class.name,
                                                         job.arguments,
                                                         job.provider_job_id,
                                                         self.class.uniqueness_mode,
                                                         job_progress,
                                                         timeout,
                                                         expires)
        true
      end

      def write_uniqueness_progress_and_addition(job)
        return unless stats_adapter.respond_to?(:write_uniqueness_progress_and_addition)

        stats_adapter.write_uniqueness_progress_and_addition(uniqueness_id,
                                                 job.queue_name,
                                                 job_progress)
      end

      def clean_uniqueness(job)
        return unless stats_adapter.respond_to?(:clean_uniqueness)

        stats_adapter.clean_uniqueness(uniqueness_id, job.queue_name)
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
        end

        def perform_later_forced(*args)
          job = job_or_instantiate(*args)
          job.unique_as_skipped = true
          job.enqueue
        end
      end
    end
  end
end

# include the extension
ActiveJob::Base.send(:include, ActiveJob::Unique::Extension)
