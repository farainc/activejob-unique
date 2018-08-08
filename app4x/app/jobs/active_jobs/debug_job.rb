class ActiveJobs::DebugJob < ApplicationJob
  queue_as :default

  def perform(*args)
    Sidekiq.redis_pool.with do |conn|


      byebug
    end

    byebug
  end

  before_enqueue do |job|
  end

  before_perform do |job|
  end

  PROGRESS_STATS_SEPARATOR = 0x1E.chr
  PROGRESS_STATS_PREFIX = :job_progress_stats
  
  DAY_SCORE_BASE = 100_000_000_000_000
  QUEUE_SCORE_BASE = 10_000_000_000_000
  UNIQUENESS_ID_SCORE_BASE = 10_000

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

  def job_progress_state
    "#{PROGRESS_STATS_PREFIX}:state"
  end

  def job_progress_state_logs
    "#{job_progress_state}:logs"
  end

  def job_progress_state_logs_key(job_name)
    "#{job_progress_state_logs}#{PROGRESS_STATS_SEPARATOR}#{job_name}"
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
end
