require 'sidekiq'

require_relative 'adapter_api_base'
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
          include ActiveJob::Unique::Adapters::SidekiqAdapter::AdapterApiBase
          include ActiveJob::Unique::Adapters::SidekiqAdapter::AdapterApiStats
          include ActiveJob::Unique::Adapters::SidekiqAdapter::AdapterApiState
          include ActiveJob::Unique::Adapters::SidekiqAdapter::AdapterApiLogging
          include ActiveJob::Unique::Adapters::SidekiqAdapter::AdapterApiCleanup
        end
      end
    end
  end
end
