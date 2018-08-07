require 'sidekiq'
require_relative 'sidekiq/server'

module ActiveJob
  module Unique
    module Web
      module SidekiqWeb
        extend ActiveSupport::Autoload

        PROGRESS_STATS_SEPARATOR = 0x1E.chr
        PROGRESS_STATS_PREFIX = :job_progress_stats

        DAY_SCORE_BASE = 100_000_000_000_000
        QUEUE_SCORE_BASE = 10_000_000_000_000
        UNIQUENESS_ID_SCORE_BASE = 10_000

        class << self
          def sequence_today
            Time.now.utc.to_date.strftime('%Y%m%d').to_i
          end

          def job_progress_stats
            "#{PROGRESS_STATS_PREFIX}:stats"
          end

          def job_progress_stats_jobs
            "#{job_progress_stats}:jobs"
          end

          def job_progress_stats_job_key(job_name, queue_name, progress_stage)
            "#{job_name}#{PROGRESS_STATS_SEPARATOR}#{queue_name}#{PROGRESS_STATS_SEPARATOR}#{progress_stage}"
          end

          def regroup_job_progress_stats(job_progress_stats_key, job_names, conn)
            stats_job_group = {}

            conn.hscan_each(job_progress_stats_key, count: 1000) do |name, value|
              job_name, queue_name, progress_stage = name.to_s.split(PROGRESS_STATS_SEPARATOR)
              next unless job_names.include?(job_name)

              stage, progress = progress_stage.split('_')
              next if queue_name.to_s.empty? || stage.to_s.empty? || progress.to_s.empty?

              stats_job_group[job_name] ||= {}
              stats_job_group[job_name][queue_name] ||= {}
              stats_job_group[job_name][queue_name]['enqueue'] ||= {}
              stats_job_group[job_name][queue_name]['perform'] ||= {}

              stats_job_group[job_name][queue_name][stage][progress] = value
            end

            stats_job_group
          end

          def job_progress_state
            "#{PROGRESS_STATS_PREFIX}:state"
          end

          def job_progress_state_logs
            "#{job_progress_state}:logs"
          end

          def job_progress_state_logs_key(job_name)
            "#{job_progress_state_logs}#{PROGRESS_STATS_SEPARATOR}#{job_name}"
          end

          def job_progress_state_key(job_name, uniqueness_id, queue_name, stage)
            [
              job_progress_state,
              job_name,
              queue_name,
              stage,
              uniqueness_id
            ].join(PROGRESS_STATS_SEPARATOR)
          end

          def job_progress_state_uniqueness_keys(conn, job_names)
            uniquess_keys = {}
            i = 0

            conn.scan_each(match: "#{job_progress_state}#{PROGRESS_STATS_SEPARATOR}*", count: 1000) do |key|
              i += 1

              state_key_prefix, job_name, queue_name, progress_stage = key.to_s.split(PROGRESS_STATS_SEPARATOR)
              next unless job_names.include?(job_name)

              stage, progress = progress_stage.split('_')
              next if job_name.to_s.empty? || queue_name.to_s.empty? || stage.to_s.empty? || progress.to_s.empty?

              uniquess_keys[job_name] ||= {}
              uniquess_keys[job_name][queue_name] ||= {}
              uniquess_keys[job_name][queue_name][stage] ||= 0

              uniquess_keys[job_name][queue_name][stage] += 1

              # maxmium 10,000
              break if i >= 10000
            end

            uniquess_keys
          end

          def job_progress_state_log_keys(conn, job_stats_all_time)
            job_log_keys = {}

            job_stats_all_time.each do |job_name, queues|
              job_score_key = "#{job_progress_state_logs_key(job_name)}#{PROGRESS_STATS_SEPARATOR}job_score"
              next unless conn.exists(job_score_key)

              job_log_keys[job_name] = {}

              queues.keys.each do |queue_name|
                min = conn.zscore(job_score_key, "queue:#{queue_name}")
                next if min.nil?

                job_log_keys[job_name][queue_name] = true
              end
            end

            job_log_keys
          end

          def job_progress_state_group_stats(conn, job_name, count, begin_index)
            filter = "#{job_progress_state}#{PROGRESS_STATS_SEPARATOR}#{job_name}#{PROGRESS_STATS_SEPARATOR}*"
            job_stats = []
            all_keys = []

            end_index = begin_index + count - 1

            cursor, keys = conn.scan('0', match: filter, count: 1000)
            all_keys += keys

            while all_keys.size < end_index && cursor != "0"
              cursor, keys = conn.scan(cursor, match: filter, count: 10)
              all_keys += keys
            end
            return [0, job_stats] if all_keys.size.zero?

            next_page_availabe = cursor != "0" || all_keys.size > end_index + 1

            keys = all_keys[begin_index..end_index]

            values = conn.mget(*keys)

            keys.each_with_index do |key, i|
              state_key_prefix, _, queue_name, progress_stage, uniqueness_id = key.to_s.split(PROGRESS_STATS_SEPARATOR)

              stage, progress = progress_stage.split('_')
              next if queue_name.to_s.empty? || stage.to_s.empty? || progress.to_s.empty?

              stats = {
                queue: queue_name,
                stage: stage,
                progress_stage: progress_stage,
                uniqueness_id: uniqueness_id
              }

              timestamp, job_id = values[i].to_s.split(PROGRESS_STATS_SEPARATOR)
              stats[:expires] = Time.at(timestamp.to_f).utc
              stats[:job_id] = job_id

              job_stats << stats
            end

            [next_page_availabe, job_stats]
          end

          def job_progress_state_log_uniqueness_ids(conn, day, job_name, queue_name, count, begin_index)
            job_stats = []
            next_page_availabe = false

            job_score_key = "#{job_progress_state_logs_key(job_name)}#{PROGRESS_STATS_SEPARATOR}job_score"
            return [false, job_stats] unless conn.exists(job_score_key)

            job_log_key = "#{job_progress_state_logs_key(job_name)}#{PROGRESS_STATS_SEPARATOR}job_logs"
            return [false, job_stats] unless conn.exists(job_log_key)

            day_score = (day % 9) * DAY_SCORE_BASE

            queue_id_score = conn.zscore(job_score_key, "queue:#{queue_name}")
            queue_id_score = (queue_id_score % 9) * QUEUE_SCORE_BASE

            [next_page_availabe, job_stats]
          end

          def cleanup_job_progress_state_group(conn, job_name)
            cursor = 0
            filter = "#{job_progress_state}#{PROGRESS_STATS_SEPARATOR}#{job_name}#{PROGRESS_STATS_SEPARATOR}*"

            loop do
              cursor, keys = conn.scan(cursor, match: filter, count: 100)
              conn.del(*keys)

              break if cursor == "0"
            end

            true
          end

          def cleanup_job_progress_state_one(conn, job_name, uniqueness_id, queue_name, stage)
            conn.del(job_progress_state_key(job_name, uniqueness_id, queue_name, stage))

            true
          end
        end

        autoload :Server
      end
    end
  end
end
