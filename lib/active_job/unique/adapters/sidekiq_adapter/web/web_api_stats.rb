require 'sidekiq'

module ActiveJob
  module Unique
    module Adapters
      module SidekiqAdapter
        module Web
          module WebApiStats
            extend ActiveSupport::Concern

            module ClassMethods
              def query_job_progress_stats_job_names(job_names, queue_name_filter, current_page)
                matched_job_names = []
                queue_name_jobs_field_key = "queue_name_jobs:#{queue_name_filter}"

                Sidekiq.redis_pool.with do |conn|
                  job_progress_stats_key = job_progress_stats

                  # read matched_job_names from redis cache
                  matched_job_names = conn.hget(job_progress_stats_key, queue_name_jobs_field_key).split(PROGRESS_STATS_SEPARATOR) if current_page > 1

                  if matched_job_names.size == 0
                    matched_job_name_collection = {}
                    match_filter = "*#{PROGRESS_STATS_SEPARATOR}#{queue_name_filter}#{PROGRESS_STATS_SEPARATOR}*"

                    conn.hscan(job_progress_stats_key, match: match_filter) do |key, value|
                      job_name, _, _ = key.to_s.split(PROGRESS_STATS_SEPARATOR)
                      next unless job_names.include?(job_name)

                      matched_job_name_collection[job_name] = true
                    end

                    matched_job_names = job_names.reject{|job_name| !matched_job_name_collection.key?(job_name) }

                    # save matched_job_names to redis cache
                    conn.hset(job_progress_stats_key, queue_name_jobs_field_key, matched_job_names.join(PROGRESS_STATS_SEPARATOR))
                  end
                end

                matched_job_names
              end

              def regroup_job_progress_stats_today(job_names, queue_name_filter)
                regroup_job_progress_stats(job_names, queue_name_filter, sequence_today)
              end

              def regroup_job_progress_stats(job_names, queue_name_filter, today = nil)
                stats_job_group = {}

                Sidekiq.redis_pool.with do |conn|
                  job_progress_stats_key = job_progress_stats
                  job_progress_stats_key = "#{job_progress_stats_key}:#{today}" if today

                  match_filter = "*#{PROGRESS_STATS_SEPARATOR}#{queue_name_filter}#{PROGRESS_STATS_SEPARATOR}*"

                  conn.hscan(job_progress_stats_key, match: match_filter, count: 1000) do |key, value|
                    job_name, queue_name, progress_stage = key.to_s.split(PROGRESS_STATS_SEPARATOR)
                    next unless job_names.include?(job_name)

                    stats_job_group[job_name] ||= {}
                    stats_job_group[job_name][queue_name] ||= {}
                    stats_job_group[job_name][queue_name]['enqueue'] ||= {}
                    stats_job_group[job_name][queue_name]['perform'] ||= {}

                    stage, progress = progress_stage.split('_')
                    next if stage.to_s.empty? || progress.to_s.empty?

                    stats_job_group[job_name][queue_name][stage][progress] = value
                  end
                end

                stats_job_group
              end

              #end ClassMethods
            end
          end
        end
      end
    end
  end
end
