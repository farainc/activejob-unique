require_relative 'uniqueness_adapter/adapter_api'
require_relative 'uniqueness_adapter/adapter_api_stats'
require_relative 'uniqueness_adapter/adapter_api_state'
require_relative 'uniqueness_adapter/adapter_api_logging'
require_relative 'uniqueness_adapter/adapter_api_cleanup'

require_relative 'uniqueness_adapter/queue_adapter'

module ActiveJob
  module Unique
    module Adapters
      module UniquenessAdapter
        extend ActiveSupport::Autoload


        autoload :AdapterApi
        autoload :AdapterApiStats
        autoload :AdapterApiState
        autoload :AdapterApiLogging
        autoload :AdapterApiCleanup
      end
    end
  end
end
