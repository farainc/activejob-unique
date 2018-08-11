require 'active_job/unique/web/sidekiq_web_api'

class ActiveJobs::DebugJob < ApplicationJob
  include ActiveJob::Unique::Web::SidekiqWebApi
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

  before_enqueue do |job|
  end

  before_perform do |job|
  end

end
