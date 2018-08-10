require 'active_support/concern'
require 'active_job/base'

require_relative 'api_base'
require_relative 'api_stats'
require_relative 'api_state'
require_relative 'api_logging'

module ActiveJob
  module Unique
    class Api
      include ActiveJob::Unique::ApiBase
      include ActiveJob::Unique::ApiStats
      include ActiveJob::Unique::ApiState
      include ActiveJob::Unique::ApiLogging

      class << self
        def ensure_progress_stage_state(job)
          return if job.uniqueness_skipped
          return unless valid_uniqueness_mode?(job.uniqueness_mode)

          case job.uniqueness_progress_stage
          when PROGRESS_STAGE_ENQUEUE_PROCESSING, PROGRESS_STAGE_ENQUEUE_PROCESSED, PROGRESS_STAGE_PERFORM_PROCESSING
            set_progress_stage_state(job)
          when PROGRESS_STAGE_ENQUEUE_FAILED
            cleanup_progress_state_stage(job)
            cleanup_progress_stage_state_flag(job, PROGRESS_STAGE_ENQUEUE_PROCESSING)
          when PROGRESS_STAGE_PERFORM_PROCESSED, PROGRESS_STAGE_PERFORM_FAILED
            cleanup_progress_state_stage(job)
            cleanup_progress_stage_state_flag(job, PROGRESS_STAGE_PERFORM_PROCESSING)
          end
        end

        # for both enqueue/perform stage
        def another_job_is_processing?(job, progress_stage)
          timestamp = getset_progress_stage_state_flag(job, progress_stage)

          return false if timestamp < Time.now.utc.to_f

          job.uniqueness_skipped_reason = "[#{job.uniqueness_progress_stage_group}] another_job_in_processing"

          true
        end

        # enqueue stage
        def another_job_enqueued?(job)
          progress_stage_state, timestamp = get_progress_stage_state(job)

          timestamp = timestamp.to_f
          return false if timestamp.zero?

          return false unless [PROGRESS_STAGE_ENQUEUE_PROCESSING, PROGRESS_STAGE_ENQUEUE_PROCESSED].include?(progress_stage_state.to_s.to_sym)

          if job.queue_adapter.uniqueness_another_job_in_queue?(
            job.queue_name,
            timestamp)

            job.uniqueness_skipped_reason = "[#{job.uniqueness_progress_stage_group}] another_job_in_queue"

            true
          else
            false
          end
        end

        def allow_enqueue_processing?(job)
          return true if job.uniqueness_skipped
          return true unless valid_uniqueness_mode?(job.uniqueness_mode)
          return true if perform_only_uniqueness_mode?(job.uniqueness_mode)

          # skip for another job in another_job_enqueued?
          return false if another_job_enqueued?(job)

          # skip for enqueue_only_uniqueness_mode &&
          # another job in perform_processing during enqueue?
          return false if !enqueue_only_uniqueness_mode?(job.uniqueness_mode) &&
                          another_job_in_performing?(job)

          # skip for job with until_timeout_uniqueness_mode? &&
          # previous job not expired yet?
          return false if until_timeout_uniqueness_mode?(job.uniqueness_mode) &&
                          another_job_not_expired_yet?(job)

          # skip if another job is_processing?
          return false if another_job_is_processing?(job, PROGRESS_STAGE_ENQUEUE_PROCESSING)

          true
        end

        # perform stage
        def another_job_in_performing?(job)
          progress_stage_state, timestamp = get_progress_stage_state(job)

          timestamp = timestamp.to_f
          return false if timestamp.zero?

          return false unless PROGRESS_STAGE_PERFORM_PROCESSING == progress_stage_state.to_s.to_sym

          if job.queue_adapter.uniqueness_another_job_in_worker?(
            job.class.name,
            job.queue_name,
            job.uniqueness_id,
            job.job_id)

            job.uniqueness_skipped_reason = "[#{job.uniqueness_progress_stage_group}] another_job_in_worker"

            true
          else
            false
          end
        end

        def allow_perform_processing?(job)
          return true if job.uniqueness_skipped
          return true unless valid_uniqueness_mode?(job.uniqueness_mode)
          return true if enqueue_only_uniqueness_mode?(job.uniqueness_mode)

          # skip for another job in perform_processing?
          return false if another_job_in_performing?(job)

          # skip for job with until_timeout_uniqueness_mode? &&&
          # previous job not expired yet?
          return false if until_timeout_uniqueness_mode?(job.uniqueness_mode) &&
                          another_job_not_expired_yet?(job)

          # skip if another job is_processing?
          return false if another_job_is_processing?(job, PROGRESS_STAGE_PERFORM_PROCESSING)

          true
        end

        def calculate_until_timeout_uniqueness_mode_expires(job)
          return unless until_timeout_uniqueness_mode?(job.uniqueness_mode)
          job.uniqueness_expires = job.uniqueness_expiration.from_now.utc.to_f
        end

        def get_until_timeout_uniqueness_mode_expiration(job)
          state_key = job_progress_stage_state_key(
            job.class.name,
            job.queue_name,
            job.uniqueness_id,
            PROGRESS_STAGE_PERFORM_TIMEOUT
          )

          job.queue_adapter.uniqueness_get_progress_stage_state_flag(state_key).to_f
        end

        def set_until_timeout_uniqueness_mode_expiration(job)
          calculate_until_timeout_uniqueness_mode_expires(job) if job.uniqueness_expires.blank?

          expires = job.uniqueness_expires.to_f
          return if expires < Time.now.utc.to_f

          state_key = job_progress_stage_state_key(
            job.class.name,
            job.queue_name,
            job.uniqueness_id,
            PROGRESS_STAGE_PERFORM_TIMEOUT
          )

          state_value = job.uniqueness_expires.to_f

          expiration = expires.to_i - Time.now.utc.to_i
          expiration += 10

          job.queue_adapter.uniqueness_set_progress_stage_state_flag(state_key, state_value)
          job.queue_adapter.uniqueness_expire_progress_stage_state_flag(state_key, expiration)
        end

        def another_job_not_expired_yet?(job)
          timestamp = get_until_timeout_uniqueness_mode_expiration(job)
          return false if timestamp < Time.now.utc.to_f

          job.uniqueness_skipped_reason = "[#{job.uniqueness_progress_stage_group}] another_job_not_expired_yet"

          true
        end
      end
    end
  end
end
