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

        def sequence_day(now)
          now.to_date.strftime('%Y%m%d').to_i
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

        def job_progress_state_key(job_name, uniqueness_id, queue_name, stage)
          [
            job_progress_state,
            job_name,
            queue_name,
            stage,
            uniqueness_id
          ].join(PROGRESS_STATS_SEPARATOR)
        end

        def progress_stats_initialize(job)
          job.queue_adapter.uniqueness_progress_stats_initialize(
            job_progress_stats_jobs,
            job.class.name
          )
        end

        def incr_progress_stats(job)
          now = Time.now.utc

          # incr stats
          job_key = job_progress_stats_job_key(job.class.name, job.queue_name, job.uniqueness_progress_stage)

          job.queue_adapter.uniqueness_incr_progress_stats(
            job_progress_stats,
            job_key,
            sequence_day(now))
        end

        def cleanup_progress_stats(job, time)
          job.queue_adapter.uniqueness_cleanup_progress_stats("#{job_progress_stats}:#{sequence_day(time)}")
        end

        def job_progress_state
          "#{PROGRESS_STATS_PREFIX}:state"
        end

        def job_progress_state_debug
          "#{job_progress_state}:debug"
        end

        def ensure_cleanup_progress_state(job, progress_stage)
          return if job.uniqueness_skipped
          return unless valid_uniqueness_mode?(job.uniqueness_mode)

          timestamp, job_id = getset_progress_state(job, progress_stage)

          cleanup_progress_state(job, progress_stage) if job_id == job.job_id
        end

        def cleanup_progress_state(job, progress_stage)
          state_key = job_progress_state_key(
            job.class.name,
            job.uniqueness_id,
            job.queue_name,
            progress_stage)

          # queue_adapter.getset_progress_state(state_key, state_value)
          job.queue_adapter.uniqueness_expire_progress_state(state_key, 0)
        end

        def getset_progress_state(job, progress_stage, readonly = false)
          state_key = job_progress_state_key(
            job.class.name,
            job.uniqueness_id,
            job.queue_name,
            progress_stage)

          # get progress_stage
          return job.queue_adapter.uniqueness_get_progress_state(state_key).to_s.split(":") if readonly

          # getset progress_stage
          expiration = PROGRESS_STATE_EXPIRATION
          state_value = "#{expiration.from_now.utc.to_f.to_s}#{PROGRESS_STATS_SEPARATOR}#{job.job_id}"

          state = job.queue_adapter.uniqueness_getset_progress_state(state_key, state_value)
          job.queue_adapter.uniqueness_expire_progress_state(state_key, expiration)

          state.to_s.split(":")
        end

        def set_progress_state_debug_data(job)
          return false unless job.uniqueness_debug

          # job.queue_adapter.uniqueness_set_progress_state_debug_data
          # job_progress_state_debug
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
          job.queue_adapter.uniqueness_another_job_in_queue?(job.uniqueness_id, job.queue_name)
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
          job.queue_adapter.uniqueness_another_job_in_worker?(job.uniqueness_id, job.queue_name, job.job_id)
        end

        def get_until_timeout_uniqueness_mode_expiration(job)
          state_key = job_progress_state_key(
            job.class.name,
            job.uniqueness_id,
            job.queue_name,
            PROGRESS_STAGE_PERFORM_EXPIRED)

          Time.at(job.queue_adapter.uniqueness_get_progress_state(state_key).to_f).utc
        end

        def set_until_timeout_uniqueness_mode_expiration(job)
          expires = Time.at(job.uniqueness_expires.to_f).utc
          return if expires < Time.now.utc

          state_key = job_progress_state_key(
            job.class.name,
            job.uniqueness_id,
            job.queue_name,
            PROGRESS_STAGE_PERFORM_EXPIRED)

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
