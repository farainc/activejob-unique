require 'active_support/concern'

module ActiveJob
  module Unique
    module Adapters
      module UniquenessAdapter
        module AdapterApiStats
          extend ActiveSupport::Concern

          module ClassMethods
            def initialize_progress_stats(stats_jobs_key, job_name)
            end

            def incr_progress_stats(stats_key, field_name, day)
            end

          end
          # end ClassMethods

        end
      end
    end
  end
end
