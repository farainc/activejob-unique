require 'active_support/concern'
require 'active_job/base'

require_relative 'compatible'
require_relative 'api'

module ActiveJob
  module Unique
    module Core
      extend ActiveSupport::Concern

      included do
        include ActiveJob::Unique::Compatible

        class_attribute :uniqueness_mode
        class_attribute :uniqueness_expiration
        class_attribute :uniqueness_debug
        class_attribute :uniqueness_debug_limits

        # uniqueness attributes
        attr_accessor :uniqueness_id
        attr_accessor :uniqueness_skipped
        attr_accessor :uniqueness_progress_stage
        attr_accessor :uniqueness_mode
        attr_accessor :uniqueness_debug
        attr_accessor :uniqueness_debug_limits
        attr_accessor :uniqueness_expires
        attr_accessor :uniqueness_expiration
        attr_accessor :uniqueness_skipped_reason
        attr_accessor :uniqueness_timestamp
        attr_accessor :uniqueness_progress_stage_group

        def initialize(*args)
          super(*args)

          @uniqueness_id = Digest::MD5.hexdigest(args.inspect.to_s)
          @uniqueness_mode ||= self.class.uniqueness_mode
          @uniqueness_debug ||= self.class.uniqueness_debug
          @uniqueness_debug_limits ||= self.class.uniqueness_debug_limits
          @uniqueness_expiration ||= self.class.uniqueness_expiration
          @uniqueness_timestamp = Time.now.utc
          @uniqueness_progress_stage_group = PROGRESS_STAGE_ENQUEUE_GROUP
        end

        before_enqueue do |job|
          uniqueness_api.initialize_progress_stats(job)

          @uniqueness_progress_stage = PROGRESS_STAGE_ENQUEUE_ATTEMPTED
          uniqueness_api.incr_progress_stats(job)

          uniqueness_api.set_progress_stage_log_data(job)
        end

        around_enqueue do |job, block|
          r = nil

          if uniqueness_api.allow_enqueue_processing?(job)
            @uniqueness_progress_stage = PROGRESS_STAGE_ENQUEUE_PROCESSING

            uniqueness_api.calculate_until_timeout_uniqueness_mode_expires(job)
            uniqueness_api.incr_progress_stats(job)
            uniqueness_api.ensure_progress_stage_state(job)

            begin
              r = block.call
              @uniqueness_progress_stage = PROGRESS_STAGE_ENQUEUE_PROCESSED
            rescue StandardError => e # LoadError, NameError edge cases
              @uniqueness_progress_stage = PROGRESS_STAGE_ENQUEUE_FAILED

              raise e
            ensure
              uniqueness_api.incr_progress_stats(job)
              uniqueness_api.ensure_progress_stage_state(job)
            end
          else
            @uniqueness_progress_stage = PROGRESS_STAGE_ENQUEUE_SKIPPED
            uniqueness_api.incr_progress_stats(job)
          end

          r
        end

        before_perform do |job|
          @uniqueness_progress_stage_group = PROGRESS_STAGE_PERFORM_GROUP

          uniqueness_api.initialize_progress_stats(job)

          @uniqueness_progress_stage = PROGRESS_STAGE_PERFORM_ATTEMPTED
          uniqueness_api.incr_progress_stats(job)

          uniqueness_api.set_progress_stage_log_data(job)
        end

        around_perform do |job, block|
          r = nil

          if uniqueness_api.allow_perform_processing?(job)
            @uniqueness_progress_stage = PROGRESS_STAGE_PERFORM_PROCESSING

            uniqueness_api.incr_progress_stats(job)
            uniqueness_api.set_until_timeout_uniqueness_mode_expiration(job)
            uniqueness_api.ensure_progress_stage_state(job)

            begin
              r = block.call
              @uniqueness_progress_stage = PROGRESS_STAGE_PERFORM_PROCESSED
            rescue StandardError => e
              @uniqueness_progress_stage = PROGRESS_STAGE_PERFORM_FAILED

              raise e
            ensure
              uniqueness_api.incr_progress_stats(job)
              uniqueness_api.ensure_progress_stage_state(job)
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

      def queue_adapter_uniqueness_api
        if queue_adapter.respond_to?(:uniqueness_api)
          queue_adapter.uniqueness_api
        else
          ActiveJob::Unique::Adapters::UniquenessAdapter::AdapterApi
        end
      end

      def uniqueness_api
        self.class.uniqueness_api
      end

      # add your static(class) methods here
      module ClassMethods
        def uniqueness_api
          ActiveJob::Unique::Api
        end

        # Set unique job options:
        #   unique_for [option (boolean:integer:duration:string:symbol), debug_flag (boolean), debug_limits (integer)]
        #
        #   MODE:
        #
        #   #1. UNIQUENESS_MODE_WHILE_EXECUTING
        #       avoid same job executed in the same queue at the same time
        #
        #   #2. UNIQUENESS_MODE_UNTIL_TIMEOUT
        #       avoid same job executed in the same queue after job start execute in amount of time
        #
        #   #3. UNIQUENESS_MODE_UNTIL_EXECUTING
        #       avoid same job enqueued to same queue at the same time
        #
        #   #4. UNIQUENESS_MODE_UNTIL_AND_WHILE_EXECUTING
        #       avoid same job enqueued and executed in the same queue at the same time
        #
        #   EXMAPLE:
        #
        #   class MyJob < ActiveJob::Base
        #     #1. [UNIQUENESS_MODE_UNTIL_AND_WHILE_EXECUTING], default unique flag
        #     unique_for true
        #
        #     #2. [UNIQUENESS_MODE_UNTIL_TIMEOUT], set an integer seconds
        #     unique_for 300
        #
        #     #3. [UNIQUENESS_MODE_UNTIL_TIMEOUT], set time duration
        #     unique_for 1.hour
        #
        #     #4. [UNIQUENESS_MODE_WHILE_EXECUTING,
        #          UNIQUENESS_MODE_UNTIL_TIMEOUT,
        #          UNIQUENESS_MODE_UNTIL_EXECUTING,
        #          UNIQUENESS_MODE_UNTIL_AND_WHILE_EXECUTING]
        #     unique_for UNIQUENESS_MODE_WHILE_EXECUTING (string or symbol)
        #
        #     #5. [DEBUG MODE], args[1]: true, args[2]: limits (1000)
        #     unique_for true, true, 1000
        #
        #     def perform
        #       "Hello World!"
        #     end
        #   end
        #
        #   puts MyJob.new(*args).perform_now # => "Hello World!"
        def unique_for(option = nil, debug = false, debug_limits = 1000)
          # default duration for a job is 10.minutes after perform processing
          # set longer duration for long running jobs
          return false if option.blank?

          self.uniqueness_expiration = 0

          # convert duration to integer
          if option.is_a?(ActiveSupport::Duration)
            option = option.to_i
          end

          if option == true
            self.uniqueness_mode = UNIQUENESS_MODE_UNTIL_AND_WHILE_EXECUTING
          elsif option.is_a?(Integer)
            self.uniqueness_mode = UNIQUENESS_MODE_UNTIL_TIMEOUT
            self.uniqueness_expiration = option if option.positive?
          else
            self.uniqueness_mode = option.to_sym
          end

          self.uniqueness_debug = (debug == true)

          debug_limits = debug_limits.to_i.abs()
          debug_limits = 1000 if debug_limits.zero?
          self.uniqueness_debug_limits = debug_limits

          true
        end
      end
    end
  end
end

# include unique core
ActiveJob::Base.send(:include, ActiveJob::Unique::Core)
