require 'sidekiq'

module ActiveJob
  module Unique
    module Adapters
      module SidekiqAdapter
        module Web
          module WebApiStats
            extend ActiveSupport::Concern

            module ClassMethods
              def regroup_job_progress_stats_today(job_names, queue_name_filter, count)
                regroup_job_progress_stats(job_names, queue_name_filter, count, sequence_today)
              end

              def regroup_job_progress_stats(job_names, queue_name_filter, count, today = nil)
                Sidekiq.redis_pool.with do |conn|
                  job_progress_stats_key = job_progress_stats
                  job_progress_stats_key = "#{job_progress_stats_key}:#{today}" if today

                  stats_job_group = {}
                  matched_job_names = {}
                  match_filter = "*#{PROGRESS_STATS_SEPARATOR}#{queue_name_filter}#{PROGRESS_STATS_SEPARATOR}*"
                  next_page_availabe = false

                  conn.hscan_each(job_progress_stats_key, match: match_filter, count: 100) do |name, value|
                    job_name, queue_name, progress_stage = name.to_s.split(PROGRESS_STATS_SEPARATOR)
                    next unless queue_name_filter == '*' || queue_name == queue_name_filter

                    next_page_availabe ||= (matched_job_names.size == count && !matched_job_names.key?(job_name))
                    next unless job_names.include?(job_name) && (matched_job_names.size < count || matched_job_names.key?(job_name))

                    matched_job_names[job_name] = true if matched_job_names.size < count

                    stage, progress = progress_stage.split('_')
                    next if queue_name.to_s.empty? || stage.to_s.empty? || progress.to_s.empty?

                    stats_job_group[job_name] ||= {}
                    stats_job_group[job_name][queue_name] ||= {}
                    stats_job_group[job_name][queue_name]['enqueue'] ||= {}
                    stats_job_group[job_name][queue_name]['perform'] ||= {}

                    stats_job_group[job_name][queue_name][stage][progress] = value
                  end

                  [next_page_availabe, stats_job_group]
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
