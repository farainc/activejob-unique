require 'sidekiq'
require_relative '../api_base'

module ActiveJob
  module Unique
    module Web
      module SidekiqWebApi
        extend ActiveSupport::Concern

        include ActiveJob::Unique::ApiBase

        PROGRESS_STATS_SEPARATOR  = ActiveJob::Unique::ApiBase::PROGRESS_STATS_SEPARATOR
        DAY_SCORE_BASE            = ActiveJob::Unique::ApiBase::DAY_SCORE_BASE
        QUEUE_SCORE_BASE          = ActiveJob::Unique::ApiBase::QUEUE_SCORE_BASE
        DAILY_SCORE_BASE          = ActiveJob::Unique::ApiBase::DAILY_SCORE_BASE
        UNIQUENESS_ID_SCORE_BASE  = ActiveJob::Unique::ApiBase::UNIQUENESS_ID_SCORE_BASE

        module ClassMethods
          def regroup_job_progress_stats_today(conn, job_names, queue_name_filter, count)
            regroup_job_progress_stats(conn, job_names, queue_name_filter, count, sequence_today)
          end

          def regroup_job_progress_stats(conn, job_names, queue_name_filter, count, today = nil)
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

          def cleanup_expired_progress_stats(conn)
            now = Time.now.utc.to_f
            timestamp = conn.hget(job_progress_stats_cleanup, 'cleanup_expired_progress_stats').to_f
            return false if timestamp > now

            day = sequence_day(Date.yesterday)
            conn.del("#{job_progress_stats}:#{day}")

            conn.hset(job_progress_stats_cleanup, 'cleanup_expired_progress_stats', (Date.tomorrow.to_time + 300).to_f)

            true
          end

          def group_job_progress_stage_uniqueness_flag_keys(conn, job_names)
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

          def query_job_progress_stage_state_uniqueness(conn, job_name, queue_name, stage, count, begin_index)
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

          def cleanup_job_progress_state_uniqueness(conn, job_name, queue_name, stage, uniqueness_id)
            cursor = 0
            state_key = job_progress_stage_state
            match_filter = [job_name, queue_name, uniqueness_id].join(PROGRESS_STATS_SEPARATOR)

            if stage != '*'
              conn.hscan_each(state_key, match: match_filter, count: 100) do |name, value|
                next if (value =~ /^#{stage}/i).nil?
                conn.hdel(state_key, name)
              end
            else
              loop do
                cursor, key_values = conn.hscan(state_key, cursor, match: match_filter, count: 100)
                conn.hdel(state_key, *key_values.map{|kv| kv[0]})

                break if cursor == '0'
              end
            end

            true
          end

          def cleanup_expired_job_progress_state_uniqueness(conn)
            now = Time.now.utc.to_f
            timestamp = conn.hget(job_progress_stats_cleanup, 'cleanup_expired_job_progress_state_uniqueness').to_f
            return false if timestamp > now

            state_key = job_progress_stage_state

            # check 5.minutes before's
            expired_at = now - 300
            sidekiq_queues = {}
            sidekiq_workers = Sidekiq::Workers.new

            conn.hscan_each(state_key, count: 100) do |name, value|
              job_name, queue_name, uniqueness_id = name.to_s.split(PROGRESS_STATS_SEPARATOR)
              progress_stage, progress_at, job_id = value.to_s.split(PROGRESS_STATS_SEPARATOR)
              progress_at = progress_at.to_f

              # only check expired jobs
              next if progress_at > expired_at

              # skip if job existed in queue or worker
              if (progress_stage =~ /^enqueue/i) == 0
                queue = sidekiq_queues[queue_name] || Sidekiq::Queue.new(queue_name)
                next if queue.latency > (Time.now.utc.to_f - progress_at)
              elsif (progress_stage =~ /^perform/i) == 0
                next if sidekiq_workers.any? {|_p, _t, w| w['queue'] == queue_name && w['payload']['wrapped'] == job_name && w['payload']['args'][0]['uniqueness_id'] == uniqueness_id && w['payload']['args'][0]['job_id'] == job_id }
              end

              conn.hdel(state_key, name)
            end

            conn.hset(job_progress_stats_cleanup, 'cleanup_expired_job_progress_state_uniqueness', (Time.now.utc + 300).to_f)

            true
          end

          def group_job_progress_stage_processing_flag_keys(conn, job_names)
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

          def query_job_progress_stage_state_processing(conn, job_name, queue_name, uniqueness_id, count, begin_index)
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

          def cleanup_job_progress_state_processing(conn, job_name, queue_name, uniqueness_id, stage)
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

            true
          end

          def group_job_progress_stage_log_keys(conn, job_stats_all_time)
            job_log_keys = {}

            job_stats_all_time.each do |job_name, queues|
              job_score_key = "#{job_progress_stage_log_key(job_name)}#{PROGRESS_STATS_SEPARATOR}job_score"
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

          def query_job_progress_stage_log_jobs(conn, day, job_name, queue_name, uniqueness_id, count, begin_index)
            next_page_availabe = false

            job_score_key = "#{job_progress_stage_log_key(job_name)}#{PROGRESS_STATS_SEPARATOR}job_score"
            return [false, []] unless conn.exists(job_score_key)

            day_score = ((day % 8) + 1) * DAY_SCORE_BASE

            queue_id_score = conn.zscore(job_score_key, "queue:#{queue_name}").to_i
            queue_id_score = (queue_id_score % 9) * QUEUE_SCORE_BASE

            uniqueness_id_score = conn.zscore(job_score_key, "uniqueness_id:#{uniqueness_id}").to_i
            uniqueness_id_score *= UNIQUENESS_ID_SCORE_BASE

            min_score = day_score + queue_id_score + uniqueness_id_score

            max_score = if uniqueness_id_score > 0
                          min_score + UNIQUENESS_ID_SCORE_BASE
                        elsif queue_id_score > 0
                          min_score + QUEUE_SCORE_BASE
                        else
                          min_score + DAY_SCORE_BASE
                        end

            job_logs = conn.zrevrangebyscore(
              job_score_key,
              "(#{max_score}",
              min_score,
              limit: [begin_index, begin_index + count + 1]
            )

            [job_logs.size > count, job_logs[0..count - 1]]
          end

          def query_job_progress_stage_log_job_one(conn, day, job_name, queue_name, uniqueness_id, job_id)
            job_score_key = "#{job_progress_stage_log_key(job_name)}#{PROGRESS_STATS_SEPARATOR}job_score"
            return { logs: [], args: {} } unless conn.exists(job_score_key)

            job_log_key = "#{job_progress_stage_log_key(job_name)}#{PROGRESS_STATS_SEPARATOR}job_logs"
            return { logs: [], args: {} } unless conn.exists(job_log_key)

            log_data_key = job_progress_stage_log_key(job_name)
            log_data_field = "#{day % 9}#{PROGRESS_STATS_SEPARATOR}#{uniqueness_id}"

            job_id_score = conn.zscore(job_score_key, "#{queue_name}:#{uniqueness_id}:#{job_id}").to_i
            begin_index = 0
            completed = false

            job_logs = []

            loop do
              temp_logs = conn.zrangebyscore(
                job_log_key,
                job_id_score,
                "(#{job_id_score + 1}",
                limit: [begin_index, 101]
              )

              temp_logs.each do |log|
                next unless (log =~ /^#{job_id}#{PROGRESS_STATS_SEPARATOR}/i) == 0

                id, progress_stage, timestamp, reason, mode, expiration, expires, debug = log.split(PROGRESS_STATS_SEPARATOR)

                job_logs << {
                  progress_stage: progress_stage,
                  timestamp: Time.at(timestamp.to_f).utc,
                  reason: reason,
                  mode: mode,
                  expiration: expiration,
                  expires: Time.at(expires.to_f).utc,
                  debug: debug
                }
                completed = %w[enqueue_skipped
                               enqueue_failed
                               perform_skipped
                               perform_failed
                               perform_processed].include?(progress_stage)

                break if completed
              end

              # update index offset
              begin_index += temp_logs.size

              # break if completed || search to the end.
              break if completed || temp_logs.size <= 100
            end

            args = JSON.parse(conn.hget(log_data_key, log_data_field)) rescue {}

            { logs: job_logs, args: args }
          end

          def cleanup_job_progress_stage_logs(conn, day, job_name, queue_name = '*', uniqueness_id = '*')
            job_score_key = "#{job_progress_stage_log_key(job_name)}#{PROGRESS_STATS_SEPARATOR}job_score"
            return unless conn.exists(job_score_key)

            job_log_key = "#{job_progress_stage_log_key(job_name)}#{PROGRESS_STATS_SEPARATOR}job_logs"
            return unless conn.exists(job_log_key)

            day_score = ((day % 8) + 1) * DAY_SCORE_BASE

            queue_id_score = conn.zscore(job_score_key, "queue:#{queue_name}").to_i
            queue_id_score = (queue_id_score % 9) * QUEUE_SCORE_BASE

            uniqueness_id_score = conn.zscore(job_score_key, "uniqueness_id:#{uniqueness_id}").to_i
            uniqueness_id_score *= UNIQUENESS_ID_SCORE_BASE

            min_score = day_score + queue_id_score + uniqueness_id_score

            max_score = if uniqueness_id_score > 0
                          min_score + UNIQUENESS_ID_SCORE_BASE
                        elsif queue_id_score > 0
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

          def cleanup_job_progress_stage_log_one(conn, _day, job_name, queue_name, uniqueness_id, job_id)
            job_score_key = "#{job_progress_stage_log_key(job_name)}#{PROGRESS_STATS_SEPARATOR}job_score"
            return unless conn.exists(job_score_key)

            job_log_key = "#{job_progress_stage_log_key(job_name)}#{PROGRESS_STATS_SEPARATOR}job_logs"
            return unless conn.exists(job_log_key)

            job_id_score = conn.zscore(job_score_key, "#{queue_name}:#{uniqueness_id}:#{job_id}").to_i

            begin_index = 0
            completed = false

            loop do
              temp_logs = conn.zrangebyscore(
                job_log_key,
                job_id_score,
                "(#{job_id_score + 1}",
                limit: [begin_index, 101]
              )

              temp_logs.each do |log|
                next unless (log =~ /^#{job_id}#{PROGRESS_STATS_SEPARATOR}/i) == 0
                id, progress_stage, timestamp = log.split(PROGRESS_STATS_SEPARATOR)

                completed = %w[enqueue_skipped
                               enqueue_failed
                               perform_skipped
                               perform_failed
                               perform_processed].include?(progress_stage)

                conn.zrem(job_log_key, log)

                break if completed
              end

              # update index offset
              begin_index += temp_logs.size

              # break if completed || search to the end.
              break if completed || temp_logs.size <= 100
            end

            conn.zrem(job_score_key, "#{queue_name}:#{uniqueness_id}:#{job_id}")

            true
          end

          def cleanup_expired_job_progress_stage_logs(conn)
            now = Time.now.utc.to_f
            day = sequence_day(Date.today - 7)

            timestamp = conn.hget(job_progress_stats_cleanup, 'cleanup_expired_job_progress_stage_logs').to_f
            return false if timestamp > now

            conn.zrange(job_progress_stats_jobs, 0, -1).each do |job_name|
              cleanup_job_progress_stage_logs(conn, day, job_name)
            end

            conn.hset(job_progress_stats_cleanup, 'cleanup_expired_job_progress_stage_logs', (Date.tomorrow.to_time + 300).to_f)

            true
          end
        end
      end
    end
  end
end
