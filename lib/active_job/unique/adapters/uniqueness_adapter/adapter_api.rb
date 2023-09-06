require_relative 'adapter_api_stats'
require_relative 'adapter_api_state'
require_relative 'adapter_api_logging'
require_relative 'adapter_api_cleanup'

module ActiveJob
  module Unique
    module Adapters
      module UniquenessAdapter
        class AdapterApi
          include ActiveJob::Unique::ApiBase
          include ActiveJob::Unique::Adapters::UniquenessAdapter::AdapterApiStats
          include ActiveJob::Unique::Adapters::UniquenessAdapter::AdapterApiState
          include ActiveJob::Unique::Adapters::UniquenessAdapter::AdapterApiLogging
          include ActiveJob::Unique::Adapters::UniquenessAdapter::AdapterApiCleanup

          class << self
            def another_job_in_queue?(queue_name, enqueued_at)
            end

            def another_job_in_worker?(job_name, queue_name, uniqueness_id, job_id)
            end
          end

        end
      end
    end
  end
end
