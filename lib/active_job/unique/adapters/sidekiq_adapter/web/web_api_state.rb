require 'sidekiq'

module ActiveJob
  module Unique
    module Adapters
      module SidekiqAdapter
        module Web
          module WebApiState
            extend ActiveSupport::Concern

            module ClassMethods
              def group_job_progress_stage_uniqueness_flag_keys(job_names)
                Sidekiq.redis_pool.with do |conn|
                  state_key = job_progress_stage_state

                  uniqueness_keys = {}
                  i = 0

                  conn.hscan_each(state_key, count: 100) do |name, value|
                    i += 1

                    job_name, queue_name, _ = name.to_s.split(PROGRESS_STATS_SEPARATOR)
                    next unless job_names.include?(job_name)
                    next if queue_name.to_s.empty?

                    progress_stage, _, _ = value.to_s.split(PROGRESS_STATS_SEPARATOR)
                    stage, _ = progress_stage.split('_')
                    next if stage.to_s.empty?

                    uniqueness_keys[job_name] ||= {}
                    uniqueness_keys[job_name][queue_name] ||= {}
                    uniqueness_keys[job_name][queue_name][stage] ||= 0

                    uniqueness_keys[job_name][queue_name][stage] += 1

                    # maxmium 10,000
                    break if i >= 10_000
                  end

                  uniqueness_keys
                end
              end

              def query_job_progress_stage_state_uniqueness(job_name, queue_name, stage, count, begin_index)
                Sidekiq.redis_pool.with do |conn|
                  state_key = job_progress_stage_state
                  match_filter = [job_name, queue_name, "*"].join(PROGRESS_STATS_SEPARATOR)

                  i = 0
                  begin_index += 1
                  end_index = begin_index + count
                  job_stats = []
                  next_page_availabe = false

                  conn.hscan_each(state_key, match: match_filter, count: 100) do |name, value|
                    next if stage != '*' && (value =~ /^#{stage}/i).nil?

                    i += 1
                    next if i < begin_index

                    if i < end_index
                      n_job_name, n_queue_name, uniqueness_id = name.to_s.split(PROGRESS_STATS_SEPARATOR)
                      progress_stage, timestamp, job_id = value.to_s.split(PROGRESS_STATS_SEPARATOR)

                      stats = {
                        job_name: n_job_name,
                        queue: n_queue_name,
                        progress_stage: progress_stage,
                        uniqueness_id: uniqueness_id,
                        timestamp: Time.at(timestamp.to_f).utc,
                        job_id: job_id
                      }

                      job_stats << stats
                    else
                      next_page_availabe = true

                      break
                    end
                  end

                  [next_page_availabe, job_stats]
                end
              end

              def cleanup_job_progress_state_uniqueness(job_name, queue_name, stage, uniqueness_id)
                Sidekiq.redis_pool.with do |conn|
                  cursor = 0
                  state_key = job_progress_stage_state
                  match_filter = [job_name, queue_name, uniqueness_id, stage].join(PROGRESS_STATS_SEPARATOR)

                  if stage != '*'
                    conn.hscan_each(state_key, match: match_filter, count: 100) do |name, value|
                      next if (value =~ /^#{stage}/i).nil?
                      conn.hdel(state_key, name)
                    end
                  else
                    loop do
                      cursor, key_values = conn.hscan(state_key, cursor, match: match_filter, count: 100)
                      keys = key_values.map { |kv| kv[0] }
                      conn.hdel(state_key, keys) if keys.size.positive?

                      break if cursor == '0'
                    end
                  end
                end

                true
              end

              def group_job_progress_stage_processing_flag_keys(job_names)
                Sidekiq.redis_pool.with do |conn|
                  state_key = "#{job_progress_stage_state}#{PROGRESS_STATS_SEPARATOR}*"

                  processing_keys = {}
                  i = 0

                  conn.scan_each(match: state_key, count: 1000) do |key|
                    i += 1

                    _, job_name, _ = key.to_s.split(PROGRESS_STATS_SEPARATOR)
                    next unless job_names.include?(job_name)
                    next if processing_keys.include?(job_name)

                    processing_keys[job_name] = true

                    # maxmium 10,000
                    break if i >= 10_000
                  end

                  processing_keys
                end
              end

              def query_job_progress_stage_state_processing(job_name, queue_name, uniqueness_id, count, begin_index)
                Sidekiq.redis_pool.with do |conn|
                  match_filter = [job_progress_stage_state, job_name, queue_name, uniqueness_id, "*"].join(PROGRESS_STATS_SEPARATOR)

                  i = 0
                  begin_index += 1
                  end_index = begin_index + count
                  job_stats = []
                  next_page_availabe = false

                  conn.scan_each(match: match_filter, count: 100) do |name|
                    i += 1
                    next if i < begin_index

                    if i < end_index
                      _, n_job_name, n_queue_name, uniqueness_id, progress_stage = name.to_s.split(PROGRESS_STATS_SEPARATOR)

                      stats = {
                        job_name: n_job_name,
                        queue: n_queue_name,
                        progress_stage: progress_stage,
                        uniqueness_id: uniqueness_id,
                        expires: Time.at(conn.get(name).to_f).utc
                      }

                      job_stats << stats
                    else
                      next_page_availabe = true

                      break
                    end
                  end

                  [next_page_availabe, job_stats]
                end
              end

              def cleanup_job_progress_state_processing(job_name, queue_name, uniqueness_id, stage)
                Sidekiq.redis_pool.with do |conn|
                  cursor = 0
                  match_filter = [job_progress_stage_state, job_name, queue_name, uniqueness_id].join(PROGRESS_STATS_SEPARATOR)

                  if stage != '*'
                    conn.del("#{match_filter}#{PROGRESS_STATS_SEPARATOR}#{stage}")
                  else
                    loop do
                      cursor, keys = conn.scan(cursor, match: "#{match_filter}#{PROGRESS_STATS_SEPARATOR}*", count: 100)
                      conn.del(*keys) if keys

                      break if cursor == '0'
                    end
                  end
                end

                true
              end


              #end ClassMethods
            end
          end
        end
      end
    end
  end
end
