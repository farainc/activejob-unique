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

                  conn.hscan(state_key) do |key, value|
                    job_name, queue_name, _ = key.to_s.split(PROGRESS_STATS_SEPARATOR)
                    next unless job_names.include?(job_name)
                    next if queue_name.to_s.empty?

                    progress_stage, _, _ = value.to_s.split(PROGRESS_STATS_SEPARATOR)
                    stage, _ = progress_stage.split('_')
                    next if stage.to_s.empty?

                    uniqueness_keys[job_name] ||= {}
                    uniqueness_keys[job_name][queue_name] ||= {}
                    uniqueness_keys[job_name][queue_name][stage] ||= 0

                    uniqueness_keys[job_name][queue_name][stage] += 1

                    break if uniqueness_keys.size > 10_000
                  end

                  uniqueness_keys
                end
              end

              def query_job_progress_stage_state_uniqueness(job_name, queue_name, stage, count, offset)
                Sidekiq.redis_pool.with do |conn|
                  state_key = job_progress_stage_state
                  match_filter = [job_name, queue_name, "*"].join(PROGRESS_STATS_SEPARATOR)

                  job_stats = []
                  next_page_availabe = false

                  conn.hscan(state_key, offset, match: match_filter) do |key, value|
                    next_page_availabe = job_stats.size > count
                    break if job_stats.size >= count

                    offset += 1
                    next if stage != '*' && (value =~ /^#{stage}/i).nil?

                    n_job_name, n_queue_name, uniqueness_id = key.to_s.split(PROGRESS_STATS_SEPARATOR)
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

                  [next_page_availabe, job_stats[0, count], offset]
                end
              end

              def cleanup_job_progress_state_uniqueness(job_name, queue_name, stage, uniqueness_id)
                Sidekiq.redis_pool.with do |conn|
                  state_key = job_progress_stage_state
                  match_filter = [job_name, queue_name, uniqueness_id].join(PROGRESS_STATS_SEPARATOR)

                  conn.hscan(state_key, match: match_filter) do |key, value|
                    next unless /^#{stage}/ =~ value
                    conn.hdel(state_key, key)
                  end
                end

                true
              end

              def group_job_progress_stage_processing_flag_keys(job_names)
                Sidekiq.redis_pool.with do |conn|
                  state_key = "#{job_progress_stage_state}#{PROGRESS_STATS_SEPARATOR}*"

                  processing_keys = {}

                  conn.scan(match: state_key) do |key|
                    _, job_name, _ = key.to_s.split(PROGRESS_STATS_SEPARATOR)
                    next unless job_names.include?(job_name)
                    next if processing_keys.include?(job_name)

                    processing_keys[job_name] = true
                  end

                  processing_keys
                end
              end

              def query_job_progress_stage_state_processing(job_name, queue_name, uniqueness_id, count, offset)
                Sidekiq.redis_pool.with do |conn|
                  match_filter = [job_progress_stage_state, job_name, queue_name, uniqueness_id, "*"].join(PROGRESS_STATS_SEPARATOR)

                  job_stats = []

                  conn.scan(offset, match: match_filter, count: count + 1) do |key|
                    offset += 1

                    _, n_job_name, n_queue_name, uniqueness_id, progress_stage = key.to_s.split(PROGRESS_STATS_SEPARATOR)

                    stats = {
                      job_name: n_job_name,
                      queue: n_queue_name,
                      progress_stage: progress_stage,
                      uniqueness_id: uniqueness_id,
                      expires: Time.at(conn.get(key).to_f).utc
                    }

                    job_stats << stats
                  end

                  [job_stats.size > count, job_stats[0, count], offset]
                end
              end

              def cleanup_job_progress_state_processing(job_name, queue_name, uniqueness_id, stage)
                Sidekiq.redis_pool.with do |conn|
                  match_filter = [job_progress_stage_state, job_name, queue_name, uniqueness_id].join(PROGRESS_STATS_SEPARATOR)

                  if stage != '*'
                    conn.del("#{match_filter}#{PROGRESS_STATS_SEPARATOR}#{stage}")
                  else
                    loop do
                      keys = conn.scan(match: "#{match_filter}#{PROGRESS_STATS_SEPARATOR}*", count: 100)
                      break if keys.blank?

                      conn.del(*keys)
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
