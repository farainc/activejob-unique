require 'sidekiq'
require 'sidekiq/api'

require_relative 'adapter_api_stats'
require_relative 'adapter_api_state'
require_relative 'adapter_api_logging'
require_relative 'adapter_api_cleanup'

module ActiveJob
  module Unique
    module Adapters
      module SidekiqAdapter
        class AdapterApi
          include ActiveJob::Unique::ApiBase
          include ActiveJob::Unique::Adapters::SidekiqAdapter::AdapterApiStats
          include ActiveJob::Unique::Adapters::SidekiqAdapter::AdapterApiState
          include ActiveJob::Unique::Adapters::SidekiqAdapter::AdapterApiLogging
          include ActiveJob::Unique::Adapters::SidekiqAdapter::AdapterApiCleanup

          class << self
            def another_job_in_queue?(queue_name, enqueued_at)
              queue = Sidekiq::Queue.new(queue_name)
              return false if queue.size.zero?

              queue.latency > (Time.now.utc.to_f - enqueued_at)
            end

            def another_job_in_worker?(job_name, queue_name, uniqueness_id, job_id)
              Sidekiq::Workers.new.any? {|_p, _t, w| w['queue'] == queue_name && w['payload']['wrapped'] == job_name && w['payload']['args'][0]['uniqueness_id'] == uniqueness_id && w['payload']['args'][0]['job_id'] != job_id }
            end
          end

        end
      end
    end
  end
end
