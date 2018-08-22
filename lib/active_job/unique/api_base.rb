require 'active_support/concern'

module ActiveJob
  module Unique
    module ApiBase
      extend ActiveSupport::Concern

      module ClassMethods
        # uniqueness job
        def valid_uniqueness_mode?(uniqueness_mode)
          [UNIQUENESS_MODE_WHILE_EXECUTING,
           UNIQUENESS_MODE_UNTIL_EXECUTING,
           UNIQUENESS_MODE_UNTIL_AND_WHILE_EXECUTING,
           UNIQUENESS_MODE_UNTIL_TIMEOUT].include?(uniqueness_mode.to_s.to_sym)
        end

        def enqueue_only_uniqueness_mode?(uniqueness_mode)
          UNIQUENESS_MODE_UNTIL_EXECUTING == uniqueness_mode.to_s.to_sym
        end

        def perform_only_uniqueness_mode?(uniqueness_mode)
          UNIQUENESS_MODE_WHILE_EXECUTING == uniqueness_mode.to_s.to_sym
        end

        def until_timeout_uniqueness_mode?(uniqueness_mode)
          UNIQUENESS_MODE_UNTIL_TIMEOUT == uniqueness_mode.to_s.to_sym
        end

        def sequence_day(now)
          now.in_time_zone(ActiveJob::Unique::Stats.timezone).to_date.strftime('%Y%m%d').to_i
        end

        def sequence_today
          sequence_day(Time.now.utc)
        end

        def job_progress_stats
          "#{PROGRESS_STATS_PREFIX}:stats"
        end

        def job_progress_stage_state
          "#{PROGRESS_STATS_PREFIX}:state"
        end

        def job_progress_stats_cleanup
          "#{PROGRESS_STATS_PREFIX}:cleanup"
        end

        def job_progress_stats_jobs
          "#{job_progress_stats}:jobs"
        end

        def job_progress_stats_job_key(job_name, queue_name, progress_stage)
          "#{job_name}#{PROGRESS_STATS_SEPARATOR}#{queue_name}#{PROGRESS_STATS_SEPARATOR}#{progress_stage}"
        end

        def job_progress_stage_state_key(job_name, queue_name, uniqueness_id, stage)
          [
            job_progress_stage_state,
            job_name,
            queue_name,
            uniqueness_id,
            stage
          ].join(PROGRESS_STATS_SEPARATOR)
        end

        def job_progress_stage_state_field(job_name, queue_name, uniqueness_id)
          [
            job_name,
            queue_name,
            uniqueness_id
          ].join(PROGRESS_STATS_SEPARATOR)
        end

        def job_progress_stage_logs
          "#{job_progress_stage_state}:logs"
        end

        def job_progress_stage_log_key(job_name)
          "#{job_progress_stage_logs}#{PROGRESS_STATS_SEPARATOR}#{job_name}"
        end

        def ensure_job_stage_log_day_base(day)
          (day % 8 + 1) * DAY_SCORE_BASE
        end

        def ensure_job_stage_log_queue_id_base(queue_id_score)
          queue_id_score = queue_id_score.to_f
          return 0 unless queue_id_score.positive?

          ((queue_id_score - 1) % 98 + 1) * QUEUE_SCORE_BASE
        end

        def ensure_job_stage_log_uniqueness_id_base(uniqueness_id_score)
          uniqueness_id_score.to_f * UNIQUENESS_ID_SCORE_BASE
        end
      end
    end
  end
end
