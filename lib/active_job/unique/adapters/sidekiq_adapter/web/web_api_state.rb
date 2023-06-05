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
                  loop do
                    i += 1
                    values = conn.hscan(state_key, count: 1000)
                    break if values.blank?

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

                    break if i > 10
                  end

                  uniqueness_keys
                end
              end

              def query_job_progress_stage_state_uniqueness(job_name, queue_name, stage, count, begin_index)
                Sidekiq.redis_pool.with do |conn|
                  state_key = job_progress_stage_state
                  match_filter = [job_name, queue_name, "*"].join(PROGRESS_STATS_SEPARATOR)

                  job_stats = []
                  i = 0
                  end_index = begin_index + count

                  loop do
                    values = conn.hscan(state_key, match: match_filter, count: 100)
                    break if values.blank?

                    values&.each do |name, value|
                      next if stage != '*' && (value =~ /^#{stage}/i).nil?
                      i += 1
                      next if i < begin_index

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

                      break if i > end_index
                    end

                    break if job_stats.size > count
                  end

                  next_page_availabe = job_stats.size > count

                  [next_page_availabe, job_stats[0, count]]
                end
              end

              def cleanup_job_progress_state_uniqueness(job_name, queue_name, stage, uniqueness_id)
                Sidekiq.redis_pool.with do |conn|
                  state_key = job_progress_stage_state
                  match_filter = [job_name, queue_name, uniqueness_id].join(PROGRESS_STATS_SEPARATOR)

                  loop do
                    values = conn.hscan(state_key, cursor, match: match_filter, count: 1000)
                    break if values.blank?

                    keys = values&.select{|k, v| /^#{stage}/ =~ v}&.keys
                    conn.hdel(state_key, keys) if keys&.size.positive?
                  end
                end

                true
              end

              def group_job_progress_stage_processing_flag_keys(job_names)
                Sidekiq.redis_pool.with do |conn|
                  state_key = "#{job_progress_stage_state}#{PROGRESS_STATS_SEPARATOR}*"

                  processing_keys = {}
                  i = 0

                  loop do
                    values = conn.scan(match: state_key, count: 1000)
                    break if values.blank?

                    values&.each do |key|
                      _, job_name, _ = key.to_s.split(PROGRESS_STATS_SEPARATOR)
                      next unless job_names.include?(job_name)
                      next if processing_keys.include?(job_name)

                      processing_keys[job_name] = true
                    end

                    # maxmium 10,000
                    i += 1
                    break if i > 10
                  end

                  processing_keys
                end
              end

              def query_job_progress_stage_state_processing(job_name, queue_name, uniqueness_id, count, begin_index)
                Sidekiq.redis_pool.with do |conn|
                  match_filter = [job_progress_stage_state, job_name, queue_name, uniqueness_id, "*"].join(PROGRESS_STATS_SEPARATOR)

                  job_stats = []

                  conn.scan(begin_index, match: match_filter, count: count + 1) do |name|
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

                  [job_stats.size > count, job_stats[0, count]]
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
