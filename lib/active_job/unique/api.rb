require 'active_support/concern'
require 'active_job/base'

module ActiveJob
  module Unique
    class Api
      class << self
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

        def failed_uniqueness_progress_stage?(uniqueness_progress_stage)
          [PROGRESS_STAGE_ENQUEUE_FAILED, PROGRESS_STAGE_PERFORM_FAILED].include?(uniqueness_progress_stage)
        end

        def sequence_day(now)
          now.to_date.strftime('%Y%m%d').to_i
        end

        def job_progress_stats
          "#{PROGRESS_STATS_PREFIX}:stats"
        end

        def job_progress_stats_jobs
          "#{job_progress_stats}:jobs"
        end

        def job_progress_state
          "#{PROGRESS_STATS_PREFIX}:state"
        end

        def job_progress_stats_job_key(job_name, queue_name, progress_stage)
          "#{job_name}#{PROGRESS_STATS_SEPARATOR}#{queue_name}#{PROGRESS_STATS_SEPARATOR}#{progress_stage}"
        end

        def job_progress_state_logs
          "#{job_progress_state}:logs"
        end

        def job_progress_state_logs_key(job_name)
          "#{job_progress_state_logs}#{PROGRESS_STATS_SEPARATOR}#{job_name}"
        end

        def job_progress_state_key(job_name, queue_name, uniqueness_id, stage)
          [
            job_progress_state,
            job_name,
            queue_name,
            uniqueness_id,
            stage
          ].join(PROGRESS_STATS_SEPARATOR)
        end

        def progress_stats_initialize(job)
          job.queue_adapter.uniqueness_progress_stats_initialize(
            job_progress_stats_jobs,
            job.class.name
          )
        end

        def incr_progress_stats(job)
          job.uniqueness_timestamp = Time.now.utc

          # incr stats
          job_key = job_progress_stats_job_key(job.class.name, job.queue_name, job.uniqueness_progress_stage)

          job.queue_adapter.uniqueness_incr_progress_stats(
            job_progress_stats,
            job_key,
            sequence_day(job.uniqueness_timestamp)
          )

          incr_progress_state_log(job) if job.uniqueness_debug
        end

        def cleanup_progress_stats(job, time)
          job.queue_adapter.uniqueness_cleanup_progress_stats("#{job_progress_stats}:#{sequence_day(time)}")
        end

        def ensure_cleanup_enqueue_progress_state(job)
          return if job.uniqueness_skipped
          return unless valid_uniqueness_mode?(job.uniqueness_mode)

          timestamp, job_id = getset_progress_state(job, PROGRESS_STAGE_ENQUEUE_PROCESSING)
          return unless job_id == job.job_id

          return if job.uniqueness_progress_stage == PROGRESS_STAGE_ENQUEUE_PROCESSED && !another_job_in_queue?(job)

          cleanup_progress_state(job, PROGRESS_STAGE_ENQUEUE_PROCESSING)
        end

        def ensure_cleanup_perform_progress_state(job)
          return if job.uniqueness_skipped
          return unless valid_uniqueness_mode?(job.uniqueness_mode)

          return until_timeout_uniqueness_mode?(job.uniqueness_mode) &&
                 job.uniqueness_progress_stage != PROGRESS_STAGE_PERFORM_FAILED

          cleanup_progress_state(job, PROGRESS_STAGE_PERFORM_PROCESSING)
        end

        def cleanup_progress_state(job, progress_stage)
          state_key = job_progress_state_key(
            job.class.name,
            job.queue_name,
            job.uniqueness_id,
            progress_stage
          )

          state_value = "#{Time.now.utc.to_f}#{PROGRESS_STATS_SEPARATOR}#{job.job_id}"

          job.queue_adapter.uniqueness_getset_progress_state(state_key, state_value)
          job.queue_adapter.uniqueness_expire_progress_state(state_key, PROGRESS_STATE_EXPIRATION)
        end

        def getset_progress_state(job, progress_stage, readonly = false)
          state_key = job_progress_state_key(
            job.class.name,
            job.queue_name,
            job.uniqueness_id,
            progress_stage
          )

          # get progress_stage
          return job.queue_adapter.uniqueness_get_progress_state(state_key).to_s.split(PROGRESS_STATS_SEPARATOR) if readonly

          # getset progress_stage
          expiration = PROGRESS_STATE_EXPIRATION
          state_value = "#{expiration.from_now.utc.to_f}#{PROGRESS_STATS_SEPARATOR}#{job.job_id}"

          state = job.queue_adapter.uniqueness_getset_progress_state(state_key, state_value)
          job.queue_adapter.uniqueness_expire_progress_state(state_key, expiration)

          state.to_s.split(PROGRESS_STATS_SEPARATOR)
        end

        def set_progress_state_log_data(job)
          return false unless job.uniqueness_debug
          return if job.arguments.blank?

          log_data_key = job_progress_state_logs_key(job.class.name)
          log_data_field = "#{sequence_day(job.uniqueness_timestamp) % 9}#{PROGRESS_STATS_SEPARATOR}#{job.uniqueness_id}"

          job.queue_adapter.uniqueness_set_progress_state_log_data(log_data_key, log_data_field, JSON.dump(job.arguments))
        end

        def incr_progress_state_log(job)
          day = sequence_day(job.uniqueness_timestamp)

          job_name = job.class.name
          progress_stage_score = (PROGRESS_STAGE.index(job.uniqueness_progress_stage.to_sym) + 1) / 10.0

          job_score_key = "#{job_progress_state_logs_key(job_name)}#{PROGRESS_STATS_SEPARATOR}job_score"
          job_log_key = "#{job_progress_state_logs_key(job_name)}#{PROGRESS_STATS_SEPARATOR}job_logs"

          job_log_value = "#{job.job_id}#{PROGRESS_STATS_SEPARATOR}#{job.uniqueness_progress_stage}#{PROGRESS_STATS_SEPARATOR}#{job.uniqueness_timestamp.to_f}"

          job.queue_adapter.uniqueness_incr_progress_state_log(
            day,
            job_score_key,
            job.queue_name,
            job.uniqueness_id,
            job.job_id,
            progress_stage_score,
            job_log_key,
            job_log_value
          )
        end

        def uniqueness_cleanup_progress_state_logs(job, time)
          job_name = job.class.name
          day = sequence_day(time)
          job_score_key = "#{job_progress_state_logs_key(job_name)}#{PROGRESS_STATS_SEPARATOR}job_score"
          job_log_key = "#{job_progress_state_logs_key(job_name)}#{PROGRESS_STATS_SEPARATOR}job_logs"

          log_data_key = job_progress_state_logs_key(job_name)
          log_data_field_match = "#{(day % 9)}#{PROGRESS_STATS_SEPARATOR}*"

          job.queue_adapter.uniqueness_cleanup_progress_state_logs(
            day,
            job_score_key,
            job_log_key,
            log_data_key,
            log_data_field_match
          )
        end

        def another_job_is_processing?(job, progress_stage, readonly = false)
          timestamp, job_id = getset_progress_state(job, progress_stage, readonly)

          return false if job_id.blank?
          return false if timestamp.to_f < Time.now.utc.to_f

          true
        end

        def allow_enqueue_processing?(job)
          return true if job.uniqueness_skipped
          return true unless valid_uniqueness_mode?(job.uniqueness_mode)
          return true if perform_only_uniqueness_mode?(job.uniqueness_mode)

          # disallow another_job_is_processing in enqueue_stage
          if another_job_is_processing?(job, PROGRESS_STAGE_ENQUEUE_PROCESSING)
            job.uniqueness_skipped_reason = 'enqueue:another_job_in_enqueue_processing'
            return false
          end

          # disallow another_job_in_queue
          if another_job_in_queue?(job)
            job.uniqueness_skipped_reason = 'enqueue:another_job_in_queue'
            return false
          end

          # allow enqueue_only_uniqueness_mode if no another_job_in_queue
          return true if enqueue_only_uniqueness_mode?(job.uniqueness_mode)

          # disallow another_job_is_processing in perform_stage
          if another_job_is_processing?(job, PROGRESS_STAGE_PERFORM_PROCESSING, true)
            job.uniqueness_skipped_reason = 'enqueue:another_job_in_perform_processing'
            return false
          end

          # disallow another_job_in_worker
          if another_job_in_worker?(job)
            job.uniqueness_skipped_reason = 'enqueue:another_job_in_worker'
            return false
          end

          return true unless until_timeout_uniqueness_mode?(job.uniqueness_mode)

          # allow previous_job_expired?
          previous_job_expired?(job)
        end

        def another_job_in_queue?(job)
          job.queue_adapter.uniqueness_another_job_in_queue?(job.queue_name, job.uniqueness_id)
        end

        def calculate_until_timeout_uniqueness_mode_expires(job)
          return unless until_timeout_uniqueness_mode?(job.uniqueness_mode)
          job.uniqueness_expires = job.uniqueness_expiration.from_now.utc.to_f
        end

        def allow_perform_processing?(job)
          return true if job.uniqueness_skipped
          return true unless valid_uniqueness_mode?(job.uniqueness_mode)
          return true if enqueue_only_uniqueness_mode?(job.uniqueness_mode)

          # disallow another_job_is_processing in perform_stage
          if another_job_is_processing?(job, PROGRESS_STAGE_PERFORM_PROCESSING)
            job.uniqueness_skipped_reason = 'perform:another_job_in_perform_processing'
            return false
          end

          # disallow another_job_in_worker
          if another_job_in_worker?(job)
            job.uniqueness_skipped_reason = 'perform:another_job_in_worker'
            return false
          end

          return true unless until_timeout_uniqueness_mode?(job.uniqueness_mode)

          # allow previous_job_expired?
          previous_job_expired?(job)
        end

        def another_job_in_worker?(job)
          job.queue_adapter.uniqueness_another_job_in_worker?(job.queue_name, job.uniqueness_id, job.job_id)
        end

        def get_until_timeout_uniqueness_mode_expiration(job)
          state_key = job_progress_state_key(
            job.class.name,
            job.queue_name,
            job.uniqueness_id,
            PROGRESS_STAGE_PERFORM_EXPIRED
          )

          Time.at(job.queue_adapter.uniqueness_get_progress_state(state_key).to_f).utc
        end

        def set_until_timeout_uniqueness_mode_expiration(job)
          expires = Time.at(job.uniqueness_expires.to_f).utc
          return if expires < Time.now.utc

          state_key = job_progress_state_key(
            job.class.name,
            job.queue_name,
            job.uniqueness_id,
            PROGRESS_STAGE_PERFORM_EXPIRED
          )

          state_value = job.uniqueness_expires.to_f

          expiration = expires.to_i - Time.now.utc.to_i
          expiration += 10

          job.queue_adapter.uniqueness_set_progress_state(state_key, state_value)
          job.queue_adapter.uniqueness_expire_progress_state(state_key, expiration)
        end

        def previous_job_expired?(job)
          get_until_timeout_uniqueness_mode_expiration(job) < Time.now.utc
        end
      end
    end
  end
end
