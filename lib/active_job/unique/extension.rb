require 'active_support/concern'
require 'active_job/base'

module ActiveJob
  module Unique
    module Extension
      extend ActiveSupport::Concern
      DATA_SEPARATOR = 0x1E.chr.freeze

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

      def ensure_data_utf8(data)
        data.to_s.encode('utf-8', invalid: :replace, undef: :replace, replace: '')
      end

      included do
        class_attribute :uniqueness_mode
        class_attribute :uniqueness_duration
        class_attribute :uniqueness_expiration

        attr_accessor :unique_as_skipped, :uniqueness_id, :job_progress

        around_enqueue do |job, block|
          r = nil

          job_progress = JOB_PROGRESS_ENQUEUE_ATTEMPTED
          incr_job_stats(job)

          # must be keep this block
          if allow_enqueue_uniqueness?(job)
            job_progress = JOB_PROGRESS_ENQUEUE_PROCESSING
            incr_job_stats(job)

            write_uniqueness_before_enqueue(job)

            begin
              r = block.call

              job_progress = JOB_PROGRESS_PERFORM_PROCESSED
              incr_job_stats(job)

              write_uniqueness_after_enqueue(job)
            rescue StandardError => e
              job_progress = JOB_PROGRESS_ENQUEUE_FAILED
              incr_job_stats(job)

              clean_uniqueness(job)
              raise e
            end
          else
            job_progress = JOB_PROGRESS_ENQUEUE_SKIPPED
            incr_job_stats(job)
          end

          r
        end

        around_perform do |job, block|
          r = nil

          job_progress = JOB_PROGRESS_PERFORM_ATTEMPTED
          incr_job_stats(job)

          # must be keep this block
          if allow_perform_uniqueness?(job)
            job_progress = JOB_PROGRESS_PERFORM_PROCESSING
            incr_job_stats(job)

            write_uniqueness_before_perform(job)

            begin
              r = block.call

              job_progress = JOB_PROGRESS_PERFORM_PROCESSED
              incr_job_stats(job)

              write_uniqueness_after_perform(job)
            rescue StandardError => e
              job_progress = JOB_PROGRESS_PERFORM_FAILED
              incr_job_stats(job)

              clean_uniqueness(job)
              raise e
            end
          else
            job_progress = JOB_PROGRESS_PERFORM_SKIPPED
            incr_job_stats(job)
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
      def prepare_uniqueness_id(job)
        @uniqueness_id ||= Digest::MD5.hexdigest([job.queue_name, job.class.name, job.arguments].inspect.to_s)
      end

      def valid_uniqueness_mode?
        [UNIQUENESS_MODE_WHILE_EXECUTING,
         UNIQUENESS_MODE_UNTIL_EXECUTING,
         UNIQUENESS_MODE_UNTIL_AND_WHILE_EXECUTING,
         UNIQUENESS_MODE_UNTIL_TIMEOUT].include?(uniqueness_mode)
      end

      def enqueue_only_uniqueness_mode?
        UNIQUENESS_MODE_UNTIL_EXECUTING == uniqueness_mode
      end

      def until_timeout_uniqueness_mode?
        UNIQUENESS_MODE_UNTIL_TIMEOUT == uniqueness_mode
      end

      def allow_enqueue_uniqueness?(job)
        return true if job.unique_as_skipped
        return true unless valid_uniqueness_mode?

        # only allow dirty_uniqueness to enqueue
        dirty_uniqueness?(job)
      end

      def allow_perform_uniqueness?(job)
        return true if job.unique_as_skipped
        return true unless valid_uniqueness_mode?
        return true if enqueue_only_uniqueness_mode?
        return true if dirty_uniqueness?(job)

        # allow enqueue_stage job to perform as well
        return true unless stats_adapter.respond_to?(:enqueue_stage_job?)
        stats_adapter.enqueue_stage_job?(prepare_uniqueness_id(job), job.queue_name)
      end

      def dirty_uniqueness?(job)
        return true unless stats_adapter.respond_to?(:dirty_uniqueness?)
        return true if read_uniqueness(job).blank?

        stats_adapter.dirty_uniqueness?(uniqueness)
      end

      def read_uniqueness(job)
        stats_adapter.read_uniqueness(prepare_uniqueness_id(job), job.queue_name) if stats_adapter.respond_to?(:read_uniqueness)
      end

      def write_uniqueness_before_enqueue(job)
        return unless valid_uniqueness_mode?

        write_uniqueness_progress(job)
        write_uniqueness_dump(job)
      end

      def write_uniqueness_after_enqueue(job)
        return unless valid_uniqueness_mode?

        write_uniqueness_progress(job)
      end

      def write_uniqueness_before_perform(job)
        return unless valid_uniqueness_mode?

        write_uniqueness_progress(job)
        write_uniqueness_dump(job)
      end

      def write_uniqueness_after_perform(job)
        if until_timeout_uniqueness_mode?
          write_uniqueness_progress(job)
          write_uniqueness_dump(job)
        else
          clean_uniqueness(job)
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
        when JOB_PROGRESS_PERFORM_PROCESSING
          timeout = uniqueness_duration.from_now.to_i
        when JOB_PROGRESS_PERFORM_PROCESSED
          uniqueness = read_uniqueness(job)

          if uniqueness.present?
            data = ensure_data_utf8(uniqueness).split(DATA_SEPARATOR)
            progress, timeout, expires = data

            timeout = timeout.to_i
          end

          timeout = uniqueness_duration.from_now.to_i unless timeout.positive?
        end

        timeout
      end

      def calculate_expires(job)
        expires = 0

        # always use saved expiration first
        uniqueness = read_uniqueness(job)
        if uniqueness.present?
          data = ensure_data_utf8(uniqueness).split(DATA_SEPARATOR)
          progress, timeout, expires = data
          expires = expires.to_i
        end

        unless expires.positive?
          expires = uniqueness_expiration.from_now.to_i
          expires = (job.scheduled_at + uniqueness_expiration).to_i if job.scheduled_at.present?
        end

        expires
      end

      def write_uniqueness_progress(job)
        return unless stats_adapter.respond_to?(:write_uniqueness_progress)

        timeout = calculate_timeout(job)
        expires = calculate_expires(job)

        stats_adapter.write_uniqueness_progress(prepare_uniqueness_id(job),
                                                job.queue_name,
                                                job_progress,
                                                timeout,
                                                expires)
      end

      def write_uniqueness_dump(job)
        return unless stats_adapter.respond_to?(:write_uniqueness_dump)

        timeout = calculate_timeout(job)
        expires = calculate_expires(job)

        stats_adapter.write_uniqueness_dump(prepare_uniqueness_id(job),
                                            job.queue_name,
                                            job.class.name,
                                            job.arguments,
                                            job.job_id,
                                            uniqueness_mode,
                                            timeout,
                                            expires)
      end

      def clean_uniqueness(job)
        return unless stats_adapter.respond_to?(:clean_uniqueness)

        stats_adapter.clean_uniqueness(prepare_uniqueness_id(job), job.queue_name)
      end

      # add your instance methods here
      def reenqueue
        self.class.perform_later(*arguments)
      end

      # add your static(class) methods here
      module ClassMethods
        def unique_for(option = nil, expiration = 60.minutes)
          # default duration for until_timeout_uniqueness_mode
          self.uniqueness_duration = 5.minutes

          if option == true
            self.uniqueness_mode = UNIQUENESS_MODE_UNTIL_AND_WHILE_EXECUTING
          elsif option.is_a?(Integer)
            self.uniqueness_mode = UNIQUENESS_MODE_UNTIL_TIMEOUT
            self.uniqueness_duration = option if option.positive?
          else
            self.uniqueness_mode = option.to_sym
          end

          self.uniqueness_expiration = expiration
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
