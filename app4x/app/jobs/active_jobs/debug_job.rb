class ActiveJobs::DebugJob < ApplicationJob
  queue_as :default


  def perform(*args)
    Sidekiq.redis_pool.with do |conn|


      byebug
    end

    byebug
  end

  def self.debug
    Sidekiq.redis_pool.with do |conn|


      byebug
    end

    byebug
  end

  def self.cleanup
    self.queue_adapter.uniqueness_api.cleanup_expired_progress_stats
    self.queue_adapter.uniqueness_api.cleanup_expired_progress_state_uniqueness
    self.queue_adapter.uniqueness_api.cleanup_expired_progress_stage_logs

    # Sidekiq.redis_pool.with{|c| c.keys('job_progress_stats*').each{|k| c.del(k)}}
  end

  before_enqueue do |job|
  end

  before_perform do |job|
  end

end
