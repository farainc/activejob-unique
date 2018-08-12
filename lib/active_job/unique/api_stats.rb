require 'active_support/concern'

module ActiveJob
  module Unique
    module ApiStats
      extend ActiveSupport::Concern

      module ClassMethods
        def initialize_progress_stats(job)
          job.queue_adapter.uniqueness_api.initialize_progress_stats(
            job_progress_stats_jobs,
            job.class.name
          )
        end

        def incr_progress_stats(job)
          job.uniqueness_timestamp = Time.now.utc

          # incr stats
          job_key = job_progress_stats_job_key(job.class.name, job.queue_name, job.uniqueness_progress_stage)

          job.queue_adapter.uniqueness_api.incr_progress_stats(
            job_progress_stats,
            job_key,
            sequence_day(job.uniqueness_timestamp)
          )

          incr_progress_stage_log(job) if job.uniqueness_debug
        end
      end
    end
  end
end
