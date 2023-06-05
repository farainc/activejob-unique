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

                  cursor = "0"
                  loop do
                    cursor, values = conn.hscan(state_key, count: 100)

                    values&.each  do |name, value|
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
                    end

                    # maxmium 10,000
                    break if cursor == "0" || cursor >= "10000"
                  end

                  uniqueness_keys
                end
              end

              def query_job_progress_stage_state_uniqueness(job_name, queue_name, stage, count, begin_index)
                Sidekiq.redis_pool.with do |conn|
                  state_key = job_progress_stage_state
                  match_filter = [job_name, queue_name, "*"].join(PROGRESS_STATS_SEPARATOR)

                  job_stats = []

                  cursor = begin_index
                  loop do
                    cursor, values = conn.hscan(state_key, cursor, match: match_filter, count: 100)

                    values&.each do |name, value|
                      next if stage != '*' && (value =~ /^#{stage}/i).nil?

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
                    end

                    break if job_stats.size >= 100 || cursor == "0"
                  end

                  next_page_availabe = job_stats.size > 100 || cursor != "0"

                  [next_page_availabe, job_stats[0, 100]]
                end
              end

              def cleanup_job_progress_state_uniqueness(job_name, queue_name, stage, uniqueness_id)
                Sidekiq.redis_pool.with do |conn|
                  state_key = job_progress_stage_state
                  match_filter = [job_name, queue_name, uniqueness_id].join(PROGRESS_STATS_SEPARATOR)

                  cursor = "0"
                  loop do
                    cursor, values = conn.hscan(state_key, cursor, match: match_filter, count: 100)

                    keys = values&.map{|kv| kv[0] if /^#{stage}/ =~ kv[1]}.compact
                    conn.hdel(state_key, keys) if keys&.size.positive?

                    break if cursor == "0"
                  end
                end

                true
              end

              def group_job_progress_stage_processing_flag_keys(job_names)
                Sidekiq.redis_pool.with do |conn|
                  state_key = "#{job_progress_stage_state}#{PROGRESS_STATS_SEPARATOR}*"

                  processing_keys = {}

                  cursor = "0"
                  loop do
                    cursor, values = conn.scan(match: state_key, count: 1000)
                    values&.each do |key|
                      _, job_name, _ = key.to_s.split(PROGRESS_STATS_SEPARATOR)
                      next unless job_names.include?(job_name)
                      next if processing_keys.include?(job_name)

                      processing_keys[job_name] = true
                    end

                    # maxmium 10,000
                    break if cursor == "0" || cursor >= "10000"
                  end

                  processing_keys
                end
              end

              def query_job_progress_stage_state_processing(job_name, queue_name, uniqueness_id, count, begin_index)
                Sidekiq.redis_pool.with do |conn|
                  match_filter = [job_progress_stage_state, job_name, queue_name, uniqueness_id, "*"].join(PROGRESS_STATS_SEPARATOR)

                  job_stats = []

                  cursor, values = conn.scan(begin_index, match: match_filter, count: 100)
                  next_page_availabe = cursor != "0"

                  values&.each do |name|
                    _, n_job_name, n_queue_name, uniqueness_id, progress_stage = name.to_s.split(PROGRESS_STATS_SEPARATOR)

                    stats = {
                      job_name: n_job_name,
                      queue: n_queue_name,
                      progress_stage: progress_stage,
                      uniqueness_id: uniqueness_id,
                      expires: Time.at(conn.get(name).to_f).utc
                    }

                    job_stats << stats
                  end

                  [next_page_availabe, job_stats]
                end
              end

              def cleanup_job_progress_state_processing(job_name, queue_name, uniqueness_id, stage)
                Sidekiq.redis_pool.with do |conn|
                  cursor = "0"
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
