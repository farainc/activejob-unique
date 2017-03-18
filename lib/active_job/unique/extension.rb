require 'active_support/concern'
require 'active_job/base'

module ActiveJob
  module Unique
    module Extension
      extend ActiveSupport::Concern
      DATA_SEPARATOR = 0x1E.chr

      included do
        class_attribute :uniqueness_mode
        class_attribute :uniqueness_duration
        class_attribute :uniqueness_expiration

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
            @job_perform_processed = true

            write_uniqueness_after_perform(job)
            incr_job_stats(job, :perform_processed)
          else
            incr_job_stats(job, :perform_skiped)
          end

          r
        end
      end

      def perform_processing?(progress)
        progress == :perform_processing
      end

      def perform_processed?
        @job_perform_processed == true
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
        return true unless uniqueness_mode_available?
        return true if job.unique_as_skiped

        uniqueness = stats_adapter.read_uniqueness(prepare_uniqueness_id(job), job.queue_name)
        return true if uniqueness.blank?

        now = Time.now.utc.to_i
        data = uniqueness.split(DATA_SEPARATOR)

        # progress, expires, defaults
        progress, expires, defaults = data
        defaults = defaults.to_i
        expires = expires.to_i

        # allow when default expiration passed
        return true if defaults.positive? && defaults < now

        # allow when perform stage and expiration passed
        return true if stats_adapter.perform_stage?(progress) && expires.positive? && expires < now

        false
      end

      def allow_perform_uniqueness?(job)
        return unless stats_adapter.respond_to?(:read_uniqueness)

        return true unless [:while_executing, :until_and_while_executing].include?(uniqueness_mode)
        return true if job.unique_as_skiped

        uniqueness = stats_adapter.read_uniqueness(prepare_uniqueness_id(job), job.queue_name)
        return true if uniqueness.blank?

        now = Time.now.utc.to_i
        data = uniqueness.split(DATA_SEPARATOR)

        # progress, expires, defaults
        progress, expires, defaults = data
        defaults = defaults.to_i
        expires = expires.to_i

        # allow when default expiration passed
        return true if defaults.positive? && defaults < now

        # allow when perform stage and expiration passed
        return true if stats_adapter.perform_stage?(progress) && expires.positive? && expires < now

        false
      end

      def incr_job_stats(job, progress)
        return unless stats_adapter.respond_to?(:incr_job_stats)

        stats_adapter.incr_job_stats(job.queue_name,
                                     job.class.name,
                                     progress)
      end

      def allow_write_uniqueness_around_enqueue?
        [:while_executing,
         :until_executing,
         :until_and_while_executing,
         :until_timeout].include?(uniqueness_mode)
      end

      def write_uniqueness_before_enqueue(job)
        return unless stats_adapter.respond_to?(:write_uniqueness_dump)
        return unless stats_adapter.respond_to?(:write_uniqueness_progress)
        return unless allow_write_uniqueness_around_enqueue?

        expires = uniqueness_expiration.from_now.to_i

        scheduled = job.scheduled_at.present?
        if scheduled && uniqueness_mode == :until_timeout && uniqueness_duration.to_i.positive?
          expires = job.scheduled_at + uniqueness_duration
        end

        stats_adapter.write_uniqueness_dump(prepare_uniqueness_id(job),
                                            job.queue_name,
                                            job.class.name,
                                            job.arguments,
                                            job.job_id,
                                            uniqueness_mode,
                                            expires)

        stats_adapter.write_uniqueness_progress(prepare_uniqueness_id(job),
                                                job.queue_name,
                                                :enqueue_processing,
                                                expires)
      end

      def write_uniqueness_after_enqueue(job)
        return unless stats_adapter.respond_to?(:write_uniqueness_progress)
        return unless allow_write_uniqueness_around_enqueue?

        expires = uniqueness_expiration.from_now.to_i

        scheduled = job.scheduled_at.present?
        if scheduled && uniqueness_mode == :until_timeout && uniqueness_duration.to_i.positive?
          expires = job.scheduled_at + uniqueness_duration
        end

        stats_adapter.write_uniqueness_progress(prepare_uniqueness_id(job),
                                                job.queue_name,
                                                :enqueue_processed,
                                                expires)
      end

      def write_uniqueness_before_perform(job)
        return unless stats_adapter.respond_to?(:write_uniqueness_dump)
        return unless stats_adapter.respond_to?(:write_uniqueness_progress)
        return unless uniqueness_mode_available?

        expires = uniqueness_expiration.from_now.to_i

        scheduled = job.scheduled_at.present?
        if scheduled && uniqueness_mode == :until_timeout && uniqueness_duration.to_i.positive?
          expires = job.scheduled_at + uniqueness_duration
        end


        stats_adapter.write_uniqueness_dump(prepare_uniqueness_id(job),
                                            job.queue_name,
                                            job.class.name,
                                            job.arguments,
                                            job.job_id,
                                            uniqueness_mode,
                                            expires)

        stats_adapter.write_uniqueness_progress(prepare_uniqueness_id(job),
                                                job.queue_name,
                                                :perform_processing,
                                                expires)
      end

      def write_uniqueness_after_perform(job)
        if stats_adapter.respond_to?(:write_uniqueness_progress) &&
           uniqueness_mode == :until_timeout &&
           uniqueness_duration.to_i.positive?

          stats_adapter.write_uniqueness_progress(prepare_uniqueness_id(job),
                                                  job.queue_name,
                                                  :perform_processed,
                                                  uniqueness_duration.from_now.to_i)
        else
          clean_uniqueness(job)
        end
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
        def unique_for(option = nil, expiration = 30.minutes)

          if option == true
            self.uniqueness_mode = :until_and_while_executing
          elsif option.is_a?(Integer)
            self.uniqueness_mode = :until_timeout
            self.uniqueness_duration = option
          else
            self.uniqueness_mode = option.to_sym
            self.uniqueness_duration = 5.minutes if self.uniqueness_mode == :until_timeout
          end

          self.uniqueness_expiration = expiration
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
