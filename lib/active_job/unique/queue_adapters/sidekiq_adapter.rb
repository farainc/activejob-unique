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
            Sidekiq.redis_pool.with do |conn|
              conn.zadd(stats_jobs_key, [1, job_name], { incr: true })
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
            if conn.zadd(job_score_key, [0, "#{base}:#{new_id}"], { nx: true }) == 1
              conn.zadd(job_score_key, [1, base], { incr: true })
              id_score = conn.zscore(job_score_key, base)
              conn.zincrby(job_score_key, id_score, "#{base}:#{new_id}")
            else
              id_score = conn.zscore(job_score_key, "#{base}:#{new_id}")
            end

            id_score
          end

          def uniqueness_incr_progress_state_log(job_score_key,
                                                 day,
                                                 queue_name,
                                                 uniqueness_id,
                                                 job_id,
                                                 progress_stage_score,
                                                 job_log_key,
                                                 job_log_value)
            Sidekiq.redis_pool.with do |conn|
              day_score = (day % 9) * DAY_SCORE_BASE

              queue_id_score = uniqueness_incr_progress_state_log_id_score(conn, job_score_key,  'queue', queue_name)
              queue_id_score = (queue_id_score % 9) * QUEUE_SCORE_BASE

              uniqueness_id_score = uniqueness_incr_progress_state_log_id_score(conn, job_score_key,  'uniqueness_id', uniqueness_id)
              uniqueness_id_score *= UNIQUENESS_ID_SCORE_BASE

              time_score = ((Time.now.utc - Time.now.utc.midnight) / 10).to_i

              job_id_score = day_score + queue_id_score + uniqueness_id_score + time_score
              job_id_value = "job_id:#{job_id}"

              if conn.zadd(job_score_key, [job_id_score, job_id_value], { nx: true }) == 0
                job_id_score = conn.zscore(job_score_key, job_id_value)
              end

              job_log_score = job_id_score + progress_stage_score
              conn.zadd(job_log_key, [job_log_score, job_log_value], { nx: true })
            end
          end

          # def uniqueness_incr_progress_state_log(job_score_key,
          #                                          job_name_with_queue,
          #                                          uniqueness_id_score_key,
          #                                          job_id_score_key,
          #                                          progress_stage_score,
          #                                          job_debug_key,
          #                                          job_debug_value)
          #
          #   Sidekiq.redis_pool.with do |conn|
          #     if conn.zadd(job_score_key, [0, uniqueness_id_score_key], { nx: true }) == 1
          #       conn.zadd(job_score_key, [1, job_name_with_queue], { incr: true })
          #       uniqueness_id_score = conn.zscore(job_score_key, job_name_with_queue)
          #       uniqueness_id_score *= UNIQUENESS_ID_SCORE_BASE
          #       conn.zincrby(job_score_key, uniqueness_id_score, uniqueness_id_score_key)
          #     else
          #       uniqueness_id_score = conn.zscore(job_score_key, uniqueness_id_score_key)
          #     end
          #
          #     if conn.zadd(job_score_key, [0, job_id_score_key], { nx: true }) == 1
          #       conn.zadd(job_score_key, [1, uniqueness_id_score_key], { incr: true })
          #       job_id_score = conn.zscore(job_score_key, uniqueness_id_score_key)
          #       conn.zincrby(job_score_key, job_id_score, job_id_score_key)
          #     else
          #       job_id_score = conn.zscore(job_score_key, uniqueness_id_score_key)
          #     end
          #
          #     job_debug_score = job_id_score + progress_stage_score
          #
          #     conn.zadd(job_debug_key, [job_debug_score, job_debug_value], { nx: true })
          #   end
          # end

          def uniqueness_cleanup_progress_state_logs(day)
            Sidekiq.redis_pool.with do |conn|

            end
          end

          def uniqueness_another_job_in_queue?(uniqueness_id, queue_name)
            queue = Sidekiq::Queue.new(queue_name)
            return false if queue.size.zero?

            queue.any? { |job| job.item['args'][0]['uniqueness_id'] == uniqueness_id }
          end

          def uniqueness_another_job_in_worker?(uniqueness_id, queue_name, job_id)
            Sidekiq::Workers.new.any? { |_p, _t, w| w['queue'] == queue_name && w['payload']['args'][0]['uniqueness_id'] == uniqueness_id && w['payload']['args'][0]['job_id'] != job_id }
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
