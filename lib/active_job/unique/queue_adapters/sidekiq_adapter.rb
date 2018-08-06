require 'active_support/concern'
require 'sidekiq/api'
require 'sidekiq/worker'

module ActiveJob
  module Unique
    module QueueAdapters
      module SidekiqAdapter
        extend ActiveSupport::Concern

        module ClassMethods
          def uniqueness_progress_stats_initialize(stats_jobs, job_name)
            Sidekiq.redis_pool.with do |conn|
              conn.zadd(stats_jobs, [1.0, job_name], { incr: true })
            end
          end

          def uniqueness_incr_progress_stats(stats_key, job_key, day)
            Sidekiq.redis_pool.with do |conn|
              conn.multi do
                # all time
                conn.hsetnx(stats_key, job_key, 0)
                conn.hincrby(stats_key, job_key, 1)

                # daily
                conn.hsetnx("#{stats_key}:#{day}", job_key, 0)
                conn.hincrby("#{stats_key}:#{day}", job_key, 1)
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
