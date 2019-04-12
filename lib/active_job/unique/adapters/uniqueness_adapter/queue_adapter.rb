require 'active_support/concern'

require_relative 'adapter_api'

module ActiveJob
  module Unique
    module Adapters
      module UniquenessAdapter
        module QueueAdapter
          extend ActiveSupport::Concern

          module ClassMethods
            def uniqueness_api
              ActiveJob::Unique::Adapters::UniquenessAdapter::AdapterApi
            end
          end

          def uniqueness_api
            self.class.uniqueness_api
          end

        end
      end
    end
  end
end
