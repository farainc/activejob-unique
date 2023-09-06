require_relative 'adapter_api_base'
require_relative 'adapter_api_cleanup'
require_relative 'web/web_api'
require_relative 'web/web_api_stats'
require_relative 'web/web_api_state'
require_relative 'web/web_api_logging'
require_relative 'web/server'

module ActiveJob
  module Unique
    module Adapters
      module SidekiqAdapter
        module Web
        end
      end
    end
  end
end
