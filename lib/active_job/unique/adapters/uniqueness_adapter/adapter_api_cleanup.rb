module ActiveJob
  module Unique
    module Adapters
      module UniquenessAdapter
        module AdapterApiCleanup
          extend ActiveSupport::Concern

          module ClassMethods
            def cleanup_progress_stats(stats_key)
            end

            def cleanup_expired_progress_stats(force_cleanup = false)
              true
            end

            def cleanup_expired_progress_state_uniqueness(force_cleanup = false)
              true
            end

            def cleanup_expired_progress_stage_logs(force_cleanup = false)
              true
            end

            def cleanup_progress_stage_logs(day,
                                            job_score_key,
                                            job_log_key,
                                            log_data_key,
                                            log_data_field_match)

              true
            end

            # end ClassMethods
          end
        end
      end
    end
  end
end
