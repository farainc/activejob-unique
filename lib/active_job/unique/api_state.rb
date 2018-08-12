require 'active_support/concern'
require 'active_job/base'

module ActiveJob
  module Unique
    module ApiState
      extend ActiveSupport::Concern

      module ClassMethods
        def get_progress_stage_state(job)
          state_key = job_progress_stage_state
          state_field = job_progress_stage_state_field(job.class.name, job.queue_name, job.uniqueness_id)

          job.queue_adapter.uniqueness_api.get_progress_stage_state(state_key, state_field).to_s.split(PROGRESS_STATS_SEPARATOR)
        end

        def set_progress_stage_state(job)
          state_key = job_progress_stage_state
          state_field = job_progress_stage_state_field(job.class.name, job.queue_name, job.uniqueness_id)
          state_data = [job.uniqueness_progress_stage, job.uniqueness_timestamp.to_f, job.job_id].join(PROGRESS_STATS_SEPARATOR)

          job.queue_adapter.uniqueness_api.set_progress_stage_state(state_key, state_field, state_data)
        end

        def expire_progress_state_stage(job)
          state_key = job_progress_stage_state
          state_field = job_progress_stage_state_field(job.class.name, job.queue_name, job.uniqueness_id)

          job.queue_adapter.uniqueness_api.expire_progress_stage_state(state_key, state_field)
        end

        def getset_progress_stage_state_flag(job, progress_stage)
          state_key = job_progress_stage_state_key(
            job.class.name,
            job.queue_name,
            job.uniqueness_id,
            progress_stage
          )

          # getset progress_stage
          state_value = Time.now.utc.to_f + PROGRESS_STATE_EXPIRATION

          timestamp = job.queue_adapter.uniqueness_api.getset_progress_stage_state_flag(state_key, state_value).to_f
          job.queue_adapter.uniqueness_api.expire_progress_stage_state_flag(state_key, PROGRESS_STATE_EXPIRATION + 10)

          timestamp
        end

        def expire_progress_stage_state_flag(job, progress_stage)
          state_key = job_progress_stage_state_key(
            job.class.name,
            job.queue_name,
            job.uniqueness_id,
            progress_stage
          )

          job.queue_adapter.uniqueness_api.expire_progress_stage_state_flag(state_key, 0)
        end
      end
    end
  end
end
