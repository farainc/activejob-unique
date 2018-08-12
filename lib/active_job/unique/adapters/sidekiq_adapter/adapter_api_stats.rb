require 'active_support/concern'

module ActiveJob
  module Unique
    module Adapters
      module SidekiqAdapter
        module AdapterApiStats
          extend ActiveSupport::Concern

          module ClassMethods
            def initialize_progress_stats(stats_jobs_key, job_name)
              day = Time.now.utc.to_date.to_date.strftime('%y%m%d').to_i
              day_score = day * DAILY_SCORE_BASE

              Sidekiq.redis_pool.with do |conn|
                score = conn.zincrby(stats_jobs_key, 1.0, job_name)
                conn.zadd(stats_jobs_key, [day_score, job_name]) if score < day_score
              end
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
            end

          end
          # end ClassMethods

        end
      end
    end
  end
end
