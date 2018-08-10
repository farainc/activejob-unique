require 'active_support/concern'
require 'active_job/base'

module ActiveJob
  module Unique
    module ApiStats
      extend ActiveSupport::Concern

      module ClassMethods
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

          incr_progress_stage_log(job) if job.uniqueness_debug
        end

        def cleanup_progress_stats(job, time)
          job.queue_adapter.uniqueness_cleanup_progress_stats("#{job_progress_stats}:#{sequence_day(time)}")
        end
      end
    end
  end
end
