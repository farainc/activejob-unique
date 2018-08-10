require 'active_support/concern'
require 'sidekiq/api'
require 'sidekiq/worker'

module ActiveJob
  module Unique
    module QueueAdapters
      module SidekiqAdapter
        extend ActiveSupport::Concern

        module ClassMethods
          def uniqueness_progress_stats_initialize(stats_jobs_key, job_name)
            day = Time.now.utc.to_date.to_date.strftime('%y%m%d').to_i
            day_score = day * DAILY_SCORE_BASE

            Sidekiq.redis_pool.with do |conn|
              score = conn.zincrby(stats_jobs_key, 1.0, job_name)

              conn.zadd(stats_jobs_key, [day_score, job_name]) if score < day_score
            end
          end

          def uniqueness_incr_progress_stats(stats_key, field_name, day)
            Sidekiq.redis_pool.with do |conn|
              conn.multi do
                # all time
                conn.hsetnx(stats_key, field_name, 0)
                conn.hincrby(stats_key, field_name, 1)

                # daily
                conn.hsetnx("#{stats_key}:#{day}", field_name, 0)
                conn.hincrby("#{stats_key}:#{day}", field_name, 1)
              end
            end
          end

          def uniqueness_cleanup_progress_stats(stats_key)
            Sidekiq.redis_pool.with do |conn|
              conn.del(stats_key)
            end
          end

          def uniqueness_getset_progress_state(state_key, data)
            Sidekiq.redis_pool.with do |conn|
              conn.getset(state_key, data)
            end
          end

          def uniqueness_get_progress_state(state_key)
            Sidekiq.redis_pool.with do |conn|
              conn.get(state_key)
            end
          end

          def uniqueness_set_progress_state(state_key, data)
            Sidekiq.redis_pool.with do |conn|
              conn.set(state_key, data)
            end
          end

          def uniqueness_expire_progress_state(state_key, seconds)
            Sidekiq.redis_pool.with do |conn|
              conn.expire(state_key, seconds)
            end
          end

          def uniqueness_set_progress_state_log_data(log_data_key, log_data_field, log_data)
            Sidekiq.redis_pool.with do |conn|
              conn.hsetnx(log_data_key, log_data_field, log_data)
            end
          end

          def uniqueness_incr_progress_state_log_id_score(conn, job_score_key, base, new_id)
            if conn.zadd(job_score_key, [0, "#{base}:#{new_id}"], nx: true) == 1
              conn.zincrby(job_score_key, conn.zincrby(job_score_key, 1.0, base), "#{base}:#{new_id}")
            else
              conn.zincrby(job_score_key, 0.0, "#{base}:#{new_id}")
            end
          end

          def uniqueness_incr_progress_state_log(day,
                                                 job_score_key,
                                                 queue_name,
                                                 uniqueness_id,
                                                 job_id,
                                                 progress_stage_score,
                                                 job_log_key,
                                                 job_log_value)
            Sidekiq.redis_pool.with do |conn|
              day_score = ((day % 8) + 1) * DAY_SCORE_BASE

              queue_id_score = uniqueness_incr_progress_state_log_id_score(conn, job_score_key, 'queue', queue_name)
              queue_id_score = (queue_id_score % 9) * QUEUE_SCORE_BASE

              uniqueness_id_score = uniqueness_incr_progress_state_log_id_score(conn, job_score_key, 'uniqueness_id', uniqueness_id)
              uniqueness_id_score *= UNIQUENESS_ID_SCORE_BASE

              time_score = ((Time.now.utc - Time.now.utc.midnight) / 10).to_i

              job_id_score = day_score + queue_id_score + uniqueness_id_score + time_score
              job_id_value = "#{queue_name}:#{uniqueness_id}:#{job_id}"

              if conn.zadd(job_score_key, [job_id_score, job_id_value], nx: true) == 0
                job_id_score = conn.zscore(job_score_key, job_id_value)
              end

              job_log_score = job_id_score + progress_stage_score
              conn.zadd(job_log_key, [job_log_score, job_log_value], nx: true)
            end
          end

          def uniqueness_cleanup_progress_state_logs(day,
                                                     job_score_key,
                                                     job_log_key,
                                                     log_data_key,
                                                     log_data_field_match)
            Sidekiq.redis_pool.with do |conn|
              day_score = ((day % 8) + 1) * DAY_SCORE_BASE
              min_score = day_score
              max_score = day_score + DAY_SCORE_BASE - 0.1

              conn.zremrangebyscore(
                job_score_key,
                min_score,
                max_score
              )

              conn.zremrangebyscore(
                job_log_key,
                min_score,
                max_score
              )

              cursor = '0'

              loop do
                cursor, hash = conn.hscan(log_data_key, match: log_data_field_match, count: 100)
                conn.hdel(hash.keys)

                break if cursor == '0'
              end

              true
            end
          end

          def uniqueness_another_job_in_queue?(job_name, queue_name, uniqueness_id)
            queue = Sidekiq::Queue.new(queue_name)
            return false if queue.size.zero?

            in_queue = false

            queue.each do |job|
              next unless job_name == job.item['wrapped']
              in_queue = uniqueness_id == job.item['args'][0]['uniqueness_id']
              break if in_queue
            end

            in_queue
          end

          def uniqueness_another_job_in_worker?(job_name, queue_name, uniqueness_id, job_id)
            Sidekiq::Workers.new.any? {|_p, _t, w| w['queue'] == queue_name && w['payload']['wrapped'] == job_name && w['payload']['args']['uniqueness_id'] == uniqueness_id && w['payload']['args']['job_id'] != job_id }
          end
        end

        def uniqueness_progress_stats_initialize(*args)
          self.class.uniqueness_progress_stats_initialize(*args)
        end

        def uniqueness_incr_progress_stats(*args)
          self.class.uniqueness_incr_progress_stats(*args)
        end

        def uniqueness_cleanup_progress_stats(*args)
          self.class.uniqueness_cleanup_progress_stats(*args)
        end

        def uniqueness_getset_progress_state(*args)
          self.class.uniqueness_getset_progress_state(*args)
        end

        def uniqueness_get_progress_state(*args)
          self.class.uniqueness_get_progress_state(*args)
        end

        def uniqueness_set_progress_state(*args)
          self.class.uniqueness_set_progress_state(*args)
        end

        def uniqueness_expire_progress_state(*args)
          self.class.uniqueness_expire_progress_state(*args)
        end

        def uniqueness_set_progress_state_log_data(*args)
          self.class.uniqueness_set_progress_state_log_data(*args)
        end

        def uniqueness_incr_progress_state_log(*args)
          self.class.uniqueness_incr_progress_state_log(*args)
        end

        def uniqueness_cleanup_progress_state_logs(*args)
          self.class.uniqueness_cleanup_progress_state_logs(*args)
        end

        def uniqueness_another_job_in_queue?(*args)
          self.class.uniqueness_another_job_in_queue?(*args)
        end

        def uniqueness_another_job_in_worker?(*args)
          self.class.uniqueness_another_job_in_worker?(*args)
        end
      end
    end
  end
end

ActiveJob::QueueAdapters::SidekiqAdapter.send(:include, ActiveJob::Unique::QueueAdapters::SidekiqAdapter)
