require 'active_support/concern'
require 'active_job/base'

module ActiveJob
  module Unique
    module Extension
      extend ActiveSupport::Concern
      DATA_SEPARATOR = 0x1E.chr

      included do
        class_attribute :uniqueness_mode
        class_attribute :uniqueness_timeout

        attr_accessor :unique_as_skiped, :uniqueness_id

        around_enqueue do |job, block|
          r = nil

          incr_job_stats(job, :enqueue_attempted)

          # must be keep this block
          if allow_enqueue_uniqueness?(job)
            write_uniqueness_before_enqueue(job)
            incr_job_stats(job, :enqueue_processing)

            begin
              r = block.call
            rescue StandardError => e
              incr_job_stats(job, :enqueue_failed)

              clean_uniqueness(job)
              raise e
            end

            write_uniqueness_after_enqueue(job)
            incr_job_stats(job, :enqueue_processed)
          else
            incr_job_stats(job, :enqueue_skiped)
          end

          r
        end

        around_perform do |job, block|
          r = nil

          incr_job_stats(job, :perform_attempted)

          cleanup_uniqueness_before_perform(job)

          # must be keep this block
          if allow_perform_uniqueness?(job)
            write_uniqueness_before_perform(job)
            incr_job_stats(job, :perform_processing)

            begin
              r = block.call
            rescue StandardError => e
              incr_job_stats(job, :perform_failed)

              clean_uniqueness(job)
              raise e
            end

            write_uniqueness_after_perform(job)
            incr_job_stats(job, :perform_processed)
          else
            incr_job_stats(job, :perform_skiped)
          end

          r
        end
      end

      def stats_adapter
        self.class.queue_adapter
      end

      # uniqueness job
      def prepare_uniqueness_id(job)
        @uniqueness_id ||= Digest::MD5.hexdigest([job.queue_name, job.class.name, job.arguments].inspect.to_s)
      end

      def uniqueness_mode_available?
        [:while_executing,
         :until_executing,
         :until_and_while_executing,
         :until_timeout].include?(uniqueness_mode)
      end

      def allow_enqueue_uniqueness?(job)
        return true unless stats_adapter.respond_to?(:read_uniqueness)
        return true unless stats_adapter.respond_to?(:dirty_uniqueness?)

        return true unless uniqueness_mode_available?
        return true if job.unique_as_skiped

        uniqueness = stats_adapter.read_uniqueness(prepare_uniqueness_id(job), job.queue_name)
        return true if uniqueness.blank?

        allow = false
        data = uniqueness.split(DATA_SEPARATOR)

        # timeout, scheduled, progress
        timeout, scheduled, _progress = data
        timeout = timeout.to_i
        scheduled = scheduled == 'true'

        if timeout.zero? || Time.now.utc.to_i > timeout
          allow = stats_adapter.dirty_uniqueness?(job.queue_name, scheduled)
        end

        allow
      end

      def allow_perform_uniqueness?(job)
        return unless stats_adapter.respond_to?(:read_uniqueness)

        return true unless [:while_executing, :until_and_while_executing].include?(uniqueness_mode)
        return true if job.unique_as_skiped

        uniqueness = stats_adapter.read_uniqueness(prepare_uniqueness_id(job), job.queue_name)
        return true if uniqueness.blank?

        false
      end

      def incr_job_stats(job, progress)
        return unless stats_adapter.respond_to?(:incr_job_stats)

        stats_adapter.incr_job_stats(job.queue_name,
                                     job.class.name,
                                     progress)
      end

      def allow_write_uniqueness_around_enqueue?
        [:until_executing,
         :until_and_while_executing,
         :until_timeout].include?(uniqueness_mode)
      end

      def write_uniqueness_before_enqueue(job)
        return unless stats_adapter.respond_to?(:write_uniqueness_dump)
        return unless stats_adapter.respond_to?(:write_uniqueness_progress)

        return unless allow_write_uniqueness_around_enqueue?

        timeout = 0
        scheduled = job.scheduled_at.present?

        if scheduled && uniqueness_mode == :until_timeout && uniqueness_timeout.to_i.positive?
          timeout = job.scheduled_at + uniqueness_timeout
        end

        stats_adapter.write_uniqueness_dump(prepare_uniqueness_id(job),
                                            job.queue_name,
                                            job.class.name,
                                            job.arguments,
                                            job.job_id,
                                            uniqueness_mode,
                                            timeout,
                                            scheduled)

        stats_adapter.write_uniqueness_progress(prepare_uniqueness_id(job),
                                                job.queue_name,
                                                :enqueue_processing,
                                                timeout,
                                                scheduled)
      end

      def write_uniqueness_after_enqueue(job)
        return unless stats_adapter.respond_to?(:write_uniqueness_progress)

        return unless allow_write_uniqueness_around_enqueue?

        timeout = 0
        scheduled = job.scheduled_at.present?

        if scheduled && uniqueness_mode == :until_timeout && uniqueness_timeout.to_i.positive?
          timeout = job.scheduled_at + uniqueness_timeout
        end

        stats_adapter.write_uniqueness_progress(prepare_uniqueness_id(job),
                                                job.queue_name,
                                                :enqueue_processed,
                                                timeout,
                                                scheduled)
      end

      def write_uniqueness_before_perform(job)
        return unless stats_adapter.respond_to?(:write_uniqueness_dump)
        return unless stats_adapter.respond_to?(:write_uniqueness_progress)

        if uniqueness_mode_available?
          stats_adapter.write_uniqueness_dump(prepare_uniqueness_id(job),
                                              job.queue_name,
                                              job.class.name,
                                              job.arguments,
                                              job.job_id,
                                              uniqueness_mode)
        end

        stats_adapter.write_uniqueness_progress(prepare_uniqueness_id(job),
                                                job.queue_name,
                                                :perform_processing)
      end

      def write_uniqueness_after_perform(job)
        if uniqueness_mode == :until_timeout && uniqueness_timeout.to_i.positive?
          return unless stats_adapter.respond_to?(:write_uniqueness_progress)

          timeout = uniqueness_timeout.seconds.from_now.utc.to_i
          stats_adapter.write_uniqueness_progress(prepare_uniqueness_id(job),
                                                  job.queue_name,
                                                  :perform_processed,
                                                  timeout)
        else
          clean_uniqueness(job)
        end
      end

      def cleanup_uniqueness_before_perform(job)
        return unless [:while_executing, :until_and_while_executing].include?(uniqueness_mode)

        clean_uniqueness(job)
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
        def unique_for(option = nil, &block)
          if block_given?
            self.uniqueness_mode = block
            return
          end

          if option == true
            self.uniqueness_mode = :until_and_while_executing
          elsif option.is_a?(Integer)
            self.uniqueness_mode = :until_timeout
            self.uniqueness_timeout = option
          else
            self.uniqueness_mode = option
          end
        end

        def perform_later_forced(*args)
          job = job_or_instantiate(*args)
          job.unique_as_skiped = true
          job.enqueue
        end
      end
    end
  end
end

# include the extension
ActiveJob::Base.send(:include, ActiveJob::Unique::Extension)
