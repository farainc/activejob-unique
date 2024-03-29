require 'sidekiq'

module ActiveJob
  module Unique
    module Adapters
      module SidekiqAdapter
        module Web
          module WebApiLogging
            extend ActiveSupport::Concern

            module ClassMethods

              def group_job_progress_stage_log_keys(job_stats_all_time)
                Sidekiq.redis_pool.with do |conn|
                  job_log_keys = {}

                  job_stats_all_time.each do |job_name, queues|
                    job_score_key = "#{job_progress_stage_log_key(job_name)}#{PROGRESS_STATS_SEPARATOR}job_score"
                    next unless conn.exists?(job_score_key)

                    job_log_keys[job_name] = {}

                    queues.keys&.each do |queue_name|
                      min = conn.zscore(job_score_key, "queue:#{queue_name}").to_f
                      next if min.nil?

                      job_log_keys[job_name][queue_name] = true
                    end
                  end

                  job_log_keys
                end
              end

              def query_job_progress_stage_log_jobs(day, job_name, queue_name, uniqueness_id, count, offset)
                Sidekiq.redis_pool.with do |conn|
                  job_score_key = "#{job_progress_stage_log_key(job_name)}#{PROGRESS_STATS_SEPARATOR}job_score"
                  return [false, [], 0] unless conn.exists?(job_score_key)

                  day_score = ensure_job_stage_log_day_base(day)

                  queue_id_score = conn.zscore("#{job_score_key}:#{day}", "queue:#{queue_name}").to_f
                  queue_id_score = ensure_job_stage_log_queue_id_base(queue_id_score)

                  uniqueness_id_score = conn.zscore("#{job_score_key}:#{day}", "uniqueness_id:#{uniqueness_id}").to_f
                  uniqueness_id_score = ensure_job_stage_log_uniqueness_id_base(uniqueness_id_score)

                  min_score = day_score + queue_id_score + uniqueness_id_score

                  max_score = if uniqueness_id_score.positive?
                                min_score + UNIQUENESS_ID_SCORE_BASE
                              elsif queue_id_score.positive?
                                min_score + QUEUE_SCORE_BASE
                              else
                                min_score + DAY_SCORE_BASE
                              end

                  job_logs = conn.zrevrangebyscore(
                    job_score_key,
                    "(#{max_score}",
                    min_score,
                    limit: [offset, offset + count + 1]
                  )

                  [job_logs.size > count, job_logs[0, count]]
                end
              end

              def query_job_progress_stage_log_job_one(day, job_name, queue_name, uniqueness_id, job_id)
                Sidekiq.redis_pool.with do |conn|
                  job_score_key = "#{job_progress_stage_log_key(job_name)}#{PROGRESS_STATS_SEPARATOR}job_score"
                  return { logs: [], args: {} } unless conn.exists?(job_score_key)

                  job_log_key = "#{job_progress_stage_log_key(job_name)}#{PROGRESS_STATS_SEPARATOR}job_logs"
                  return { logs: [], args: {} } unless conn.exists?(job_log_key)

                  log_data_key = job_progress_stage_log_key(job_name)
                  log_data_field = "#{sequence_day_score(day)}#{PROGRESS_STATS_SEPARATOR}#{uniqueness_id}"

                  job_id_score = conn.zscore(job_score_key, "#{queue_name}:#{uniqueness_id}:#{job_id}").to_f

                  begin_index = 0
                  completed = false

                  job_logs = []

                  loop do
                    temp_logs = conn.zrangebyscore(
                      job_log_key,
                      job_id_score,
                      "(#{job_id_score + 1}",
                      limit: [0, 101]
                    )

                    temp_logs.each do |log|
                      next unless /^#{job_id}#{PROGRESS_STATS_SEPARATOR}/i.match(log)

                      _, progress_stage, timestamp, reason, mode, expiration, expires, debug = log.split(PROGRESS_STATS_SEPARATOR)

                      job_logs << {
                        progress_stage: progress_stage,
                        timestamp: Time.at(timestamp.to_f).utc,
                        reason: reason,
                        mode: mode,
                        expiration: expiration,
                        expires: Time.at(expires.to_f).utc,
                        debug: debug,
                        sort_key: timestamp.to_f
                      }
                    end

                    # update index offset
                    begin_index += temp_logs.size

                    # break if completed || search to the end.
                    break if temp_logs.size <= 100
                  end

                  args = JSON.parse(conn.hget(log_data_key, log_data_field)) rescue {}

                  { logs: job_logs.sort_by { |log| log[:sort_key] }, args: args }
                end
              end

              def cleanup_job_progress_stage_logs(day, job_name, queue_name = '*', uniqueness_id = '*')
                Sidekiq.redis_pool.with do |conn|
                  job_score_key = "#{job_progress_stage_log_key(job_name)}#{PROGRESS_STATS_SEPARATOR}job_score"
                  return false unless conn.exists?(job_score_key)

                  job_log_key = "#{job_progress_stage_log_key(job_name)}#{PROGRESS_STATS_SEPARATOR}job_logs"
                  return false unless conn.exists?(job_log_key)

                  day_score = ensure_job_stage_log_day_base(day)

                  queue_id_score = conn.zscore(job_score_key, "queue:#{queue_name}").to_f
                  queue_id_score = ensure_job_stage_log_queue_id_base(queue_id_score)

                  uniqueness_id_score = conn.zscore(job_score_key, "uniqueness_id:#{uniqueness_id}").to_f
                  uniqueness_id_score = ensure_job_stage_log_uniqueness_id_base(uniqueness_id_score)

                  min_score = day_score + queue_id_score + uniqueness_id_score

                  max_score = if uniqueness_id_score.positive?
                                min_score + UNIQUENESS_ID_SCORE_BASE
                              elsif queue_id_score.positive?
                                min_score + QUEUE_SCORE_BASE
                              else
                                min_score + DAY_SCORE_BASE
                              end

                  conn.zremrangebyscore(
                    job_score_key,
                    min_score,
                    "(#{max_score}"
                  )

                  conn.zremrangebyscore(
                    job_log_key,
                    min_score,
                    "(#{max_score}"
                  )

                  true
                end
              end

              def cleanup_job_progress_stage_log_one(day, job_name, queue_name, uniqueness_id, job_id)
                Sidekiq.redis_pool.with do |conn|
                  job_score_key = "#{job_progress_stage_log_key(job_name)}#{PROGRESS_STATS_SEPARATOR}job_score"
                  return false unless conn.exists?(job_score_key)

                  job_log_key = "#{job_progress_stage_log_key(job_name)}#{PROGRESS_STATS_SEPARATOR}job_logs"
                  return false unless conn.exists?(job_log_key)

                  job_id_value = "#{queue_name}:#{uniqueness_id}:#{job_id}"
                  job_id_score = conn.zscore(job_score_key, job_id_value).to_f

                  conn.zremrangebyscore(
                    job_log_key,
                    job_id_score,
                    "(#{job_id_score + 1}"
                  )

                  conn.zrem(job_score_key, job_id_value)

                  true
                end
              end

              #end ClassMethods
            end
          end
        end
      end
    end
  end
end
