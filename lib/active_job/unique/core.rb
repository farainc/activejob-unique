require 'active_support/concern'
require 'active_job/base'

module ActiveJob
  module Unique
    module Core
      extend ActiveSupport::Concern

      included do
        include ActiveJob::Unique::Stats
        include ActiveJob::Unique::Compatible
        include ActiveJob::Unique::Api

        class_attribute :uniqueness_mode
        class_attribute :uniqueness_expiration
        class_attribute :uniqueness_debug

        # uniqueness attributes
        attr_accessor :uniqueness_id
        attr_accessor :uniqueness_skipped
        attr_accessor :progress_stage
        attr_accessor :uniqueness_mode
        attr_accessor :uniqueness_debug
        attr_accessor :uniqueness_expires
        attr_accessor :uniqueness_expiration
        attr_accessor :uniqueness_skipped_reason

        before_enqueue do |job|
          @uniqueness_id = Digest::MD5.hexdigest(job.arguments.inspect.to_s)
          @uniqueness_mode ||= self.class.uniqueness_mode
          @uniqueness_debug ||= self.class.uniqueness_debug
          @uniqueness_expiration ||= self.class.uniqueness_expiration

          progress_stats_initialize(job)

          incr_progress_stats(job, PROGRESS_STAGE_ENQUEUE_ATTEMPTED)
        end

        around_enqueue do |job, block|
          r = nil

          if allow_enqueue_processing?(job)
            incr_progress_stats(job, PROGRESS_STAGE_ENQUEUE_PROCESSING)
            calculate_until_timeout_uniqueness_mode_expires(job)

            s = nil
            begin
              r = block.call
              s = PROGRESS_STAGE_ENQUEUE_PROCESSED
            rescue StandardError => e # LoadError, NameError edge cases
              s = PROGRESS_STAGE_ENQUEUE_FAILED
              raise e
            ensure
              incr_progress_stats(job, s)
              ensure_cleanup_progress_state(job, PROGRESS_STAGE_ENQUEUE_PROCESSING)
            end
          else
            incr_progress_stats(job, PROGRESS_STAGE_ENQUEUE_SKIPPED)
          end

          r
        end

        before_perform do |job|
          incr_progress_stats(job, PROGRESS_STAGE_PERFORM_ATTEMPTED)
        end

        around_perform do |job, block|
          r = nil

          if allow_perform_processing?(job)
            incr_progress_stats(job, PROGRESS_STAGE_PERFORM_PROCESSING)
            set_until_timeout_uniqueness_mode_expiration(job)

            s = nil
            begin
              r = block.call
              s = PROGRESS_STAGE_PERFORM_PROCESSED
            rescue StandardError => e
              s = PROGRESS_STAGE_PERFORM_FAILED
              raise e
            ensure
              incr_progress_stats(job, s)
              cleanup_progress_state(job, PROGRESS_STAGE_PERFORM_PROCESSING)
            end
          else
            incr_progress_stats(job, PROGRESS_STAGE_PERFORM_SKIPPED)
          end

          r
        end

      end

      # add your instance methods here
      def reenqueue
        self.class.perform_later(*arguments)
      end

      # add your static(class) methods here
      module ClassMethods
        def unique_for(option = nil, debug = false)
          # default duration for a job is 10.minutes after perform processing
          # set longer duration for long running jobs

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
