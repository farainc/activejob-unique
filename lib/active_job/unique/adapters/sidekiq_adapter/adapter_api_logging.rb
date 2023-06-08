require 'active_support/concern'

module ActiveJob
  module Unique
    module Adapters
      module SidekiqAdapter
        module AdapterApiLogging
          extend ActiveSupport::Concern

          module ClassMethods
            def set_progress_stage_log_data(log_data_key, log_data_field, log_data)
              Sidekiq.redis_pool.with do |conn|
                conn.hsetnx(log_data_key, log_data_field, log_data)
              end
            rescue StandardError => e
              Sidekiq.logger.error e
              Sidekiq.logger.error e.backtrace&.join("\n")
            end

            def incr_progress_stage_log_id_score(conn, job_score_key, base, new_id)
              if conn.zadd(job_score_key, 'NX', [0, "#{base}:#{new_id}"]) == 1
                conn.zincrby(job_score_key, conn.zincrby(job_score_key, 1.0, base), "#{base}:#{new_id}")
              else
                conn.zincrby(job_score_key, 0.0, "#{base}:#{new_id}")
              end
            rescue StandardError => e
              Sidekiq.logger.error e
              Sidekiq.logger.error e.backtrace&.join("\n")
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
              Sidekiq.redis_pool.with do |conn|
                job_id_value = "#{queue_name}:#{uniqueness_id}:#{job_id}"
                job_id_score = conn.zscore(job_score_key, job_id_value).to_f

                day_score = ensure_job_stage_log_day_base(day)

                queue_id_score = incr_progress_stage_log_id_score(conn, job_score_key, 'queue', queue_name)
                queue_id_score = ensure_job_stage_log_queue_id_base(queue_id_score)

                uniqueness_id_score = incr_progress_stage_log_id_score(conn, job_score_key, 'uniqueness_id', uniqueness_id)
                uniqueness_id_score = ensure_job_stage_log_uniqueness_id_base(uniqueness_id_score)

                if job_id_score.zero?
                  # time_score with timezone
                  now = Time.now.in_time_zone(ActiveJob::Unique::Stats.timezone)
                  time_score = ((now - now.to_time.in_time_zone(ActiveJob::Unique::Stats.timezone)) / 10).to_i

                  job_id_score = day_score + queue_id_score + uniqueness_id_score + time_score

                  job_id_score = conn.zscore(job_score_key, job_id_value).to_f if conn.zadd(job_score_key, 'NX', [job_id_score, job_id_value]).zero?
                end

                job_log_score = job_id_score + progress_stage_score
                conn.zadd(job_log_key, 'NX', [job_log_score, job_log_value])

                # remove over limits logs
                min_score = day_score
                max_score = min_score + DAY_SCORE_BASE

                loop do
                  job_score_logs = conn.zrange(
                    job_score_key,
                    "(#{max_score}",
                    min_score,
                    'REV',
                    'BYSCORE',
                    'LIMIT',
                    debug_limits,
                    100
                  )

                  job_score_logs&.each do |log|
                    temp_job_id_score = conn.zscore(job_score_key, log).to_f

                    conn.zremrangebyscore(
                      job_log_key,
                      temp_job_id_score,
                      "(#{temp_job_id_score + 1}"
                    )

                    conn.zrem(job_score_key, log)
                  end

                  break if job_score_logs.size < 100
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
