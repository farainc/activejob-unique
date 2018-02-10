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
          end

          r
        end

        around_perform do |job, block|
          r = nil

          @job_progress = JOB_PROGRESS_PERFORM_ATTEMPTED
          incr_job_stats(job)

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
          end

          r
        end
      end

      def enqueue_processing?
        @job_progress == JOB_PROGRESS_ENQUEUE_PROCESSING
      end

      def enqueue_processed?
        @job_progress == JOB_PROGRESS_ENQUEUE_PROCESSED
      end

      def enqueue_skipped?
        @job_progress == JOB_PROGRESS_ENQUEUE_SKIPPED
      end

      def perform_processing?
        @job_progress == JOB_PROGRESS_PERFORM_PROCESSING
      end

      def perform_processed?
        @job_progress == JOB_PROGRESS_PERFORM_PROCESSED
      end

      def perform_skipped?
        @job_progress == JOB_PROGRESS_PERFORM_SKIPPED
      end

      def stats_adapter
        self.class.queue_adapter
      end

      # uniqueness job
      def invalid_uniqueness_mode?
        ![UNIQUENESS_MODE_WHILE_EXECUTING,
          UNIQUENESS_MODE_UNTIL_EXECUTING,
          UNIQUENESS_MODE_UNTIL_AND_WHILE_EXECUTING,
          UNIQUENESS_MODE_UNTIL_TIMEOUT].include?(uniqueness_mode)
      end

      def enqueue_only_uniqueness_mode?
        UNIQUENESS_MODE_UNTIL_EXECUTING == uniqueness_mode
      end

      def perform_only_uniqueness_mode?
        UNIQUENESS_MODE_WHILE_EXECUTING == uniqueness_mode
      end

      def until_timeout_uniqueness_mode?
        UNIQUENESS_MODE_UNTIL_TIMEOUT == uniqueness_mode
      end

      def duplicated_job_in_worker?(job)
        Sidekiq::Workers.new.any? { |_p, _t, w| w['queue'] == job.queue_name && w['payload']['uniqueness_id'] == uniqueness_id && w['payload']['jid'] != job.job_id }
      end

      def duplicated_job_in_queue?(job)
        queue = Sidekiq::Queue.new(job.queue_name)

        return false if queue.size.zero?
        queue.any? { |job| job.item['args'][0]['uniqueness_id'] == uniqueness_id }
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
        return true if (enqueue_only_uniqueness_mode? || dirty_uniqueness?(job)) && !duplicated_job_in_queue?(job)

        false
      end

      def allow_perform_uniqueness?(job)
        return true if job.unique_as_skipped
        return true if invalid_uniqueness_mode?
        return true if enqueue_only_uniqueness_mode?

        !duplicated_job_in_worker?(job)
      end

      def dirty_uniqueness?(job)
        return true unless stats_adapter.respond_to?(:dirty_uniqueness?)

        stats_adapter.dirty_uniqueness?(uniqueness_id, read_uniqueness(job), job.queue_name)
      end

      def read_uniqueness(job)
        stats_adapter.read_uniqueness(uniqueness_id, job.queue_name) if stats_adapter.respond_to?(:read_uniqueness)
      end

      def write_uniqueness_before_enqueue(job)
        return if invalid_uniqueness_mode?

        write_uniqueness_dump(job)

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

        if until_timeout_uniqueness_mode?
          write_uniqueness_progress_and_dump(job)
        else
          clean_uniqueness(job) if should_clean_it
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
        when JOB_PROGRESS_ENQUEUE_PROCESSING, JOB_PROGRESS_ENQUEUE_PROCESSED
          timeout = uniqueness_expiration.from_now.utc.to_i
        when JOB_PROGRESS_PERFORM_PROCESSING
          timeout = uniqueness_duration.from_now.utc.to_i
        when JOB_PROGRESS_PERFORM_PROCESSED
          uniqueness = read_uniqueness(job)
          j = JSON.load(uniqueness) rescue nil
          timeout = j['t'].to_i if j.present?

          timeout = uniqueness_duration.from_now.utc.to_i unless timeout.positive?
        end

        timeout
      end

      def calculate_expires(job)
        expires = 0

        # reset expires when enqueue processing
        case job_progress
        when JOB_PROGRESS_ENQUEUE_PROCESSING, JOB_PROGRESS_ENQUEUE_PROCESSED
          expires = uniqueness_expiration.from_now.utc.to_i
          expires = (job.scheduled_at + uniqueness_expiration).to_i if job.scheduled_at.present?
        when JOB_PROGRESS_PERFORM_PROCESSING
          expires = uniqueness_expiration.from_now.utc.to_i
        else
          # always use saved expiration first
          uniqueness = read_uniqueness(job)
          j = JSON.load(uniqueness) rescue nil
          expires = j['e'].to_i if j.present?

          expires = uniqueness_expiration.from_now.utc.to_i unless expires.positive?
        end

        expires
      end

      def write_uniqueness_progress(job)
        return unless stats_adapter.respond_to?(:write_uniqueness_progress)

        timeout = calculate_timeout(job)
        expires = calculate_expires(job)

        stats_adapter.write_uniqueness_progress(uniqueness_id,
                                                job.queue_name,
                                                job.class.name,
                                                uniqueness_mode,
                                                job_progress,
                                                timeout,
                                                expires)
      end

      def write_uniqueness_dump(job)
        return unless stats_adapter.respond_to?(:write_uniqueness_dump)

        stats_adapter.write_uniqueness_dump(uniqueness_id,
                                            job.queue_name,
                                            job.class.name,
                                            job.arguments,
                                            job.job_id)
      end

      def write_uniqueness_progress_and_dump(job)
        return false unless stats_adapter.respond_to?(:write_uniqueness_progress_and_dump)

        timeout = calculate_timeout(job)
        # return false if timeout < Time.now.utc.to_i

        expires = calculate_expires(job)
        # return false if expires < Time.now.utc.to_i

        stats_adapter.write_uniqueness_progress_and_dump(uniqueness_id,
                                                         job.queue_name,
                                                         job.class.name,
                                                         job.arguments,
                                                         job.job_id,
                                                         uniqueness_mode,
                                                         job_progress,
                                                         timeout,
                                                         expires)
        true
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
          'uniqueness_mode' => uniqueness_mode,
          'unique_as_skipped' => unique_as_skipped
        }
      end

      def deserialize(job_data)
        self.job_id               = job_data['job_id']
        self.provider_job_id      = job_data['provider_job_id']
        self.queue_name           = job_data['queue_name']
        self.priority             = job_data['priority']
        self.serialized_arguments = job_data['arguments']
        self.executions           = job_data['executions']
        self.locale               = job_data['locale'] || I18n.locale.to_s
        self.uniqueness_id        = job_data['uniqueness_id']
        self.uniqueness_mode      = job_data['uniqueness_mode'].to_s.to_sym
        self.unique_as_skipped    = job_data['unique_as_skipped']
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
