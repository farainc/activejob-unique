class ActiveJobs::DebugJob < ApplicationJob
  include Sidekiq::ServerMiddleware

  queue_as :default

  def perform(*_args)
    redis_pool.with do |_conn|
      debugger
    end

    debugger
  end

  def self.debug
    redis_pool.with do |_conn|
      debugger
    end

    debugger
  end

  def self.cleanup
    queue_adapter.uniqueness_api.cleanup_expired_progress_stats
    queue_adapter.uniqueness_api.cleanup_expired_progress_state_uniqueness
    queue_adapter.uniqueness_api.cleanup_expired_progress_stage_logs
  end

  before_enqueue do |job|
  end

  before_perform do |job|
  end
end
