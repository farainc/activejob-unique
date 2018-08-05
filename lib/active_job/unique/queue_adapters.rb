require_relative 'queue_adapters/sidekiq_adapter'

module ActiveJob
  module Unique
    module QueueAdapters
      extend ActiveSupport::Autoload

      autoload :SidekiqAdapter
    end
  end
end
