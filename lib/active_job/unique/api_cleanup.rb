require 'active_support/concern'

module ActiveJob
  module Unique
    module ApiCleanup
      extend ActiveSupport::Concern

      module ClassMethods
        def cleanup_progress_stats(job, time)
          job.queue_adapter.uniqueness_api.cleanup_progress_stats("#{job_progress_stats}:#{sequence_day(time)}")
        end

        def cleanup_progress_stage_logs(job, time)
          job_name = job.class.name
          day = sequence_day(time)
          job_score_key = "#{job_progress_stage_log_key(job_name)}#{PROGRESS_STATS_SEPARATOR}job_score"
          job_log_key = "#{job_progress_stage_log_key(job_name)}#{PROGRESS_STATS_SEPARATOR}job_logs"

          log_data_key = job_progress_stage_log_key(job_name)
          log_data_field_match = "#{((day % 8) + 1)}#{PROGRESS_STATS_SEPARATOR}*"

          job.queue_adapter.uniqueness_api.cleanup_progress_stage_logs(
            day,
            job_score_key,
            job_log_key,
            log_data_key,
            log_data_field_match
          )
        end
      end
    end
  end
end
