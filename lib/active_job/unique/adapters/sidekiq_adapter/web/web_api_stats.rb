require 'sidekiq'

module ActiveJob
  module Unique
    module Adapters
      module SidekiqAdapter
        module Web
          module WebApiStats
            extend ActiveSupport::Concern

            module ClassMethods
              def regroup_job_progress_stats_today(job_names, queue_name_filter, begin_match_index, count)
                regroup_job_progress_stats(job_names, queue_name_filter, begin_match_index, count, sequence_today)
              end

              def regroup_job_progress_stats(job_names, queue_name_filter, begin_match_index, count, today = nil)
                Sidekiq.redis_pool.with do |conn|
                  job_progress_stats_key = job_progress_stats
                  job_progress_stats_key = "#{job_progress_stats_key}:#{today}" if today

                  stats_job_group = {}
                  matched_job_names = {}
                  match_filter = "*#{PROGRESS_STATS_SEPARATOR}#{queue_name_filter}#{PROGRESS_STATS_SEPARATOR}*"
                  next_page_availabe = false

                  conn.hscan_each(job_progress_stats_key, match: match_filter, count: 100) do |name, value|
                    job_name, queue_name, progress_stage = name.to_s.split(PROGRESS_STATS_SEPARATOR)

                    next if queue_name.to_s.empty?
                    next unless job_names.include?(job_name)
                    next unless queue_name_filter == '*' || queue_name == queue_name_filter

                    matched_job_names[job_name] = true
                    next unless matched_job_names.size > begin_match_index
                    next unless stats_job_group.size < count || stats_job_group.key?(job_name)

                    stats_job_group[job_name] ||= {}
                    stats_job_group[job_name][queue_name] ||= {}
                    stats_job_group[job_name][queue_name]['enqueue'] ||= {}
                    stats_job_group[job_name][queue_name]['perform'] ||= {}

                    stage, progress = progress_stage.split('_')
                    next if stage.to_s.empty? || progress.to_s.empty?

                    stats_job_group[job_name][queue_name][stage][progress] = value
                  end

                  next_page_availabe = matched_job_names.size > begin_match_index + count

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
