require 'active_support/concern'
require 'active_job/base'

module ActiveJob
  module Unique
    module Core
      extend ActiveSupport::Concern

      included do
        include ActiveJob::Unique::Compatible

        class_attribute :uniqueness_mode
        class_attribute :uniqueness_expiration
        class_attribute :uniqueness_debug

        # uniqueness attributes
        attr_accessor :uniqueness_id
        attr_accessor :uniqueness_skipped
        attr_accessor :uniqueness_progress_stage
        attr_accessor :uniqueness_mode
        attr_accessor :uniqueness_debug
        attr_accessor :uniqueness_expires
        attr_accessor :uniqueness_expiration
        attr_accessor :uniqueness_skipped_reason
        attr_accessor :uniqueness_timestamp

        before_enqueue do |job|
          @uniqueness_id = Digest::MD5.hexdigest(job.arguments.inspect.to_s)
          @uniqueness_mode ||= job.class.uniqueness_mode
          @uniqueness_debug ||= job.class.uniqueness_debug
          @uniqueness_expiration ||= job.class.uniqueness_expiration
          @uniqueness_timestamp = Time.now.utc

          uniqueness_api.progress_stats_initialize(job)

          @uniqueness_progress_stage = PROGRESS_STAGE_ENQUEUE_ATTEMPTED
          uniqueness_api.incr_progress_stats(job)

          uniqueness_api.set_progress_state_log_data(job)
        end

        around_enqueue do |job, block|
          r = nil

          if uniqueness_api.allow_enqueue_processing?(job)
            @uniqueness_progress_stage = PROGRESS_STAGE_ENQUEUE_PROCESSING

            uniqueness_api.incr_progress_stats(job)
            uniqueness_api.calculate_until_timeout_uniqueness_mode_expires(job)

            s = nil
            begin
              r = block.call
              @uniqueness_progress_stage = PROGRESS_STAGE_ENQUEUE_PROCESSED
            rescue StandardError => e # LoadError, NameError edge cases
              @uniqueness_progress_stage = PROGRESS_STAGE_ENQUEUE_FAILED

              raise e
            ensure
              uniqueness_api.incr_progress_stats(job)
              uniqueness_api.ensure_cleanup_enqueue_progress_state(job)
            end
          else
            @uniqueness_progress_stage = PROGRESS_STAGE_ENQUEUE_SKIPPED
            uniqueness_api.incr_progress_stats(job)
          end

          r
        end

        before_perform do |job|
          uniqueness_api.progress_stats_initialize(job)

          @uniqueness_progress_stage = PROGRESS_STAGE_PERFORM_ATTEMPTED
          uniqueness_api.incr_progress_stats(job)

          uniqueness_api.set_progress_state_log_data(job)
        end

        around_perform do |job, block|
          r = nil

          if uniqueness_api.allow_perform_processing?(job)
            @uniqueness_progress_stage = PROGRESS_STAGE_PERFORM_PROCESSING

            uniqueness_api.incr_progress_stats(job)
            uniqueness_api.set_until_timeout_uniqueness_mode_expiration(job)

            s = nil
            begin
              r = block.call
              @uniqueness_progress_stage = PROGRESS_STAGE_PERFORM_PROCESSED
            rescue StandardError => e
              @uniqueness_progress_stage = PROGRESS_STAGE_PERFORM_FAILED

              raise e
            ensure
              uniqueness_api.incr_progress_stats(job)
              uniqueness_api.ensure_cleanup_perform_progress_state(job)
            end
          else
            @uniqueness_progress_stage = PROGRESS_STAGE_PERFORM_SKIPPED
            uniqueness_api.incr_progress_stats(job)
          end

          r
        end
      end

      # add your instance methods here
      def reenqueue
        self.class.perform_later(*arguments)
      end

      def queue_adapter
        self.class.queue_adapter
      end

      def uniqueness_api
        self.class.uniqueness_api
      end

      # add your static(class) methods here
      module ClassMethods
        def uniqueness_api
          ActiveJob::Unique::Api
        end

        def unique_for(option = nil, debug = false)
          # default duration for a job is 10.minutes after perform processing
          # set longer duration for long running jobs
          return false if option.blank?

          self.uniqueness_expiration = 0

          if option == true
            self.uniqueness_mode = UNIQUENESS_MODE_UNTIL_AND_WHILE_EXECUTING
          elsif option.is_a?(Integer)
            self.uniqueness_mode = UNIQUENESS_MODE_UNTIL_TIMEOUT
            self.uniqueness_expiration = option if option.positive?
          else
            self.uniqueness_mode = option.to_sym
          end

          self.uniqueness_debug = debug

          true
        end
      end
    end
  end
end

# include unique core
ActiveJob::Base.send(:include, ActiveJob::Unique::Core)
