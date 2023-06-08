require 'active_support/concern'

module ActiveJob
  module Unique
    module Adapters
      module SidekiqAdapter
        module AdapterApiStats
          extend ActiveSupport::Concern

          module ClassMethods
            def initialize_progress_stats(stats_jobs_key, job_name)
              day = sequence_today
              day_score = day * DAILY_SCORE_BASE

              Sidekiq.redis_pool.with do |conn|
                score = conn.zincrby(stats_jobs_key, 1.0, job_name).to_f
                conn.zadd(stats_jobs_key, [day_score, job_name]) if score < day_score
              end
            rescue StandardError => e
              Sidekiq.logger.error e
              Sidekiq.logger.error e.backtrace&.join("\n")
            end

            def incr_progress_stats(stats_key, field_name, day)
              Sidekiq.redis_pool.with do |conn|
                conn.multi do |multi|
                  # all time
                  multi.hsetnx(stats_key, field_name, 0)
                  multi.hincrby(stats_key, field_name, 1)

                  # daily
                  multi.hsetnx("#{stats_key}:#{day}", field_name, 0)
                  multi.hincrby("#{stats_key}:#{day}", field_name, 1)
                end
              end
            rescue StandardError => e
              Sidekiq.logger.error e
              Sidekiq.logger.error e.backtrace&.join("\n")
            end
          end
          # end ClassMethods
        end
      end
    end
  end
end
