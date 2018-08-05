module ActiveJob
  module Unique
    module Stats
      def progress_stats_jobs
        PROGRESS_STATS_PREFIX
      end

      def progress_stats_job_group(job_name)
        "#{PROGRESS_STATS_PREFIX}:stats:#{job_name}"
      end

      def daily_progress_stats_job_group(job_name, day)
        "#{progress_stats_job_group(job_name)}:#{day}"
      end

      def progress_stats_job_group_key(queue_name, progress_stage)
        "#{queue_name}:#{progress_stage}"
      end

      def progress_state_job_key(job_name, uniqueness_id, queue_name, stage)
        "#{PROGRESS_STATS_PREFIX}:state:#{job_name}:#{uniqueness_id}:#{queue_name}:#{stage}"
      end
    end
  end
end
