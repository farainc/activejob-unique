require_relative 'api_base'
require_relative 'adapters/sidekiq_adapter'

module ActiveJob
  module Unique
    module Adapters
      extend ActiveSupport::Autoload

      autoload :SidekiqAdapter
    end
  end
end
