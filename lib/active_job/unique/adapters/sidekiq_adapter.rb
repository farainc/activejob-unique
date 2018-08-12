require 'sidekiq'

require_relative 'sidekiq_adapter/adapter_api'
require_relative 'sidekiq_adapter/adapter_api_stats'
require_relative 'sidekiq_adapter/adapter_api_state'
require_relative 'sidekiq_adapter/adapter_api_logging'
require_relative 'sidekiq_adapter/adapter_api_cleanup'

require_relative 'sidekiq_adapter/queue_adapter'

module ActiveJob
  module Unique
    module Adapters
      module SidekiqAdapter
        extend ActiveSupport::Autoload

        autoload :AdapterApi
        autoload :AdapterApiStats
        autoload :AdapterApiState
        autoload :AdapterApiLogging
        autoload :AdapterApiCleanup
        
        autoload :QueueAdapter
      end
    end
  end
end
