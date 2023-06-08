require 'active_support/concern'

module ActiveJob
  module Unique
    module ApiLogging
      extend ActiveSupport::Concern

      module ClassMethods
        def set_progress_stage_log_data(job)
          return false unless job.uniqueness_debug
          return if job.arguments.blank?

          log_data_key = job_progress_stage_log_key(job.class.name)
          log_data_field = "#{sequence_day_score(sequence_day(job.uniqueness_timestamp))}#{PROGRESS_STATS_SEPARATOR}#{job.uniqueness_id}"

          job.queue_adapter_uniqueness_api.set_progress_stage_log_data(log_data_key, log_data_field, JSON.dump(job.arguments))
        end

        def incr_progress_stage_log(job)
          return false unless job.uniqueness_debug

          day = sequence_day(job.uniqueness_timestamp)

          job_name = job.class.name
          progress_stage_score = (PROGRESS_STAGE.index(job.uniqueness_progress_stage.to_sym) + 1) / 10.0

          job_score_key = "#{job_progress_stage_log_key(job_name)}#{PROGRESS_STATS_SEPARATOR}job_score"
          job_log_key = "#{job_progress_stage_log_key(job_name)}#{PROGRESS_STATS_SEPARATOR}job_logs"

          job_log_values = [
            job.job_id,
            job.uniqueness_progress_stage,
            job.uniqueness_timestamp.to_f,
            job.uniqueness_skipped_reason,
            job.uniqueness_mode,
            job.uniqueness_expiration,
            job.uniqueness_expires.to_f,
            job.uniqueness_debug,
            job.uniqueness_debug_limits
          ]

          job_log_value = job_log_values.join(PROGRESS_STATS_SEPARATOR)

          job.queue_adapter_uniqueness_api.incr_progress_stage_log(
            day,
            job_score_key,
            job.queue_name,
            job.uniqueness_id,
            job.job_id,
            progress_stage_score,
            job_log_key,
            job_log_value,
            job.uniqueness_debug_limits
          )
        end
      end
    end
  end
end
