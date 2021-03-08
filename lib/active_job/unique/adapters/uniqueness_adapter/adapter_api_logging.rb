require 'active_support/concern'

module ActiveJob
  module Unique
    module Adapters
      module UniquenessAdapter
        module AdapterApiLogging
          extend ActiveSupport::Concern

          module ClassMethods
            def set_progress_stage_log_data(log_data_key, log_data_field, log_data)
            end

            def incr_progress_stage_log_id_score(conn, job_score_key, base, new_id)
            end

            def incr_progress_stage_log(day,
                                        job_score_key,
                                        queue_name,
                                        uniqueness_id,
                                        job_id,
                                        progress_stage_score,
                                        job_log_key,
                                        job_log_value,
                                        debug_limits)
            end
          end
          # end ClassMethods
        end
      end
    end
  end
end
