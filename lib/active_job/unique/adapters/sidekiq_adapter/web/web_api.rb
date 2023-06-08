require 'sidekiq'

require_relative 'web_api_stats'
require_relative 'web_api_state'
require_relative 'web_api_logging'

module ActiveJob
  module Unique
    module Adapters
      module SidekiqAdapter
        module Web
          class WebApi
            include ActiveJob::Unique::ApiBase
            include ActiveJob::Unique::Adapters::SidekiqAdapter::AdapterApiCleanup
            include ActiveJob::Unique::Adapters::SidekiqAdapter::Web::WebApiStats
            include ActiveJob::Unique::Adapters::SidekiqAdapter::Web::WebApiState
            include ActiveJob::Unique::Adapters::SidekiqAdapter::Web::WebApiLogging
          end
        end
      end
    end
  end
end
