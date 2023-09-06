require 'sidekiq'

module ActiveJob
  module Unique
    module Adapters
      module SidekiqAdapter
        module AdapterApiCleanup
          extend ActiveSupport::Concern

          module ClassMethods
            def cleanup_progress_stats(stats_key)
              Sidekiq.redis_pool.with do |conn|
                conn.del(stats_key)
              end
            end

            def cleanup_expired_progress_stats(force_cleanup = false)
              Sidekiq.redis_pool.with do |conn|
                now = Time.now.in_time_zone(ActiveJob::Unique::Stats.timezone)
                timestamp = conn.hget(job_progress_stats_cleanup, 'cleanup_expired_progress_stats').to_f
                return false if !force_cleanup && timestamp > now.to_f

                day = sequence_day(now - ONE_DAY_SECONDS)
                cleanup_progress_stats("#{job_progress_stats}:#{day}")

                next_cleanup_at = now.midnight
                conn.hset(job_progress_stats_cleanup, 'cleanup_expired_progress_stats', (next_cleanup_at + ONE_DAY_SECONDS + 4800).to_f)
              end

              true
            end

            def cleanup_expired_progress_state_uniqueness(force_cleanup = false)
              count = 0

              Sidekiq.redis_pool.with do |conn|
                now = Time.now.utc.to_f
                timestamp = conn.hget(job_progress_stats_cleanup, 'cleanup_expired_progress_state_uniqueness').to_f

                return false if !force_cleanup && timestamp > now

                state_key = job_progress_stage_state

                # check 5.minutes before's
                expired_at = now - 60

                conn.hscan_each(state_key) do |key, value|
                  job_name, queue_name, uniqueness_id = key.to_s.split(PROGRESS_STATS_SEPARATOR)
                  progress_stage, progress_at, job_id = value.to_s.split(PROGRESS_STATS_SEPARATOR)
                  progress_at = progress_at.to_f

                  # only check expired jobs
                  next if progress_at > expired_at

                  # skip if job existed in queue or worker
                  if /^enqueue/i.match?(progress_stage)
                    next if another_job_in_queue?(job_name, queue_name, uniqueness_id)
                  elsif /^perform/i.match?(progress_stage)
                    next if another_job_in_worker?(job_name, queue_name, uniqueness_id, job_id)
                  end

                  conn.hdel(state_key, name)
                  count += 1
                end

                conn.hset(job_progress_stats_cleanup, 'cleanup_expired_progress_state_uniqueness', (Time.now.utc + 60).to_f)
              end

              count
            end

            def cleanup_expired_progress_stage_logs(force_cleanup = false)
              count = 0
              Sidekiq.redis_pool.with do |conn|
                now = Time.now.in_time_zone(ActiveJob::Unique::Stats.timezone)
                day = sequence_day(now - 7 * ONE_DAY_SECONDS)

                timestamp = conn.hget(job_progress_stats_cleanup, 'cleanup_expired_progress_stage_logs').to_f
                return false if !force_cleanup && timestamp > now.to_f

                conn.zrange(job_progress_stats_jobs, 0, -1)&.each do |job_name|
                  job_score_key = "#{job_progress_stage_log_key(job_name)}#{PROGRESS_STATS_SEPARATOR}job_score"
                  job_log_key = "#{job_progress_stage_log_key(job_name)}#{PROGRESS_STATS_SEPARATOR}job_logs"
                  log_data_key = job_progress_stage_log_key(job_name)
                  log_data_field_match = "#{sequence_day_score(day)}#{PROGRESS_STATS_SEPARATOR}*"

                  count += cleanup_progress_stage_logs(day, job_score_key, job_log_key, log_data_key, log_data_field_match)
                end

                next_cleanup_at = now.midnight
                conn.hset(job_progress_stats_cleanup, 'cleanup_expired_progress_stage_logs', (next_cleanup_at + 5400).to_f)
              end

              count
            end

            def cleanup_progress_stage_logs(day,
                                            job_score_key,
                                            job_log_key,
                                            log_data_key,
                                            log_data_field_match)

              count = 0
              job_score_day_key = "#{job_score_key}:#{day}"

              Sidekiq.redis_pool.with do |conn|
                day_score = ensure_job_stage_log_day_base(day)

                min_score = day_score
                max_score = day_score + DAY_SCORE_BASE - 0.1

                conn.zremrangebyscore(
                  job_score_day_key,
                  '-inf',
                  '+inf'
                )

                conn.zremrangebyscore(
                  job_score_key,
                  min_score,
                  max_score
                )

                conn.zremrangebyscore(
                  job_log_key,
                  min_score,
                  max_score
                )
                conn.hscan_each(log_data_key, match: log_data_field_match) do |k, _v|
                  conn.hdel(log_data_key, k)
                  count += 1
                end
              end

            count
            end

            # end ClassMethods
          end
        end
      end
    end
  end
end
