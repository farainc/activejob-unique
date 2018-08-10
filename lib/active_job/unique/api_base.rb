require 'active_support/concern'
require 'active_job/base'

module ActiveJob
  module Unique
    module ApiBase
      extend ActiveSupport::Concern

      PROGRESS_STATE_EXPIRATION = 30.seconds
      PROGRESS_STATS_SEPARATOR = 0x1E.chr
      PROGRESS_STATS_PREFIX = :job_progress_stats

      DAY_SCORE_BASE    = 100_000_000_000_000
      QUEUE_SCORE_BASE  = 10_000_000_000_000
      DAILY_SCORE_BASE  = 1_000_000_000
      UNIQUENESS_ID_SCORE_BASE = 10_000

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
          now.to_date.strftime('%Y%m%d').to_i
        end

        def sequence_today
          sequence_day(Time.now.utc)
        end

        def job_progress_stats
          "#{PROGRESS_STATS_PREFIX}:stats"
        end

        def job_progress_stats_jobs
          "#{job_progress_stats}:jobs"
        end

        def job_progress_stats_job_key(job_name, queue_name, progress_stage)
          "#{job_name}#{PROGRESS_STATS_SEPARATOR}#{queue_name}#{PROGRESS_STATS_SEPARATOR}#{progress_stage}"
        end

        def job_progress_stage_state
          "#{PROGRESS_STATS_PREFIX}:state"
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

        def cleanup_all_progress_stats(job)
          job.queue_adapter.uniqueness_cleanup_all_progress_stats("#{PROGRESS_STATS_PREFIX}*")
        end
      end
    end
  end
end
