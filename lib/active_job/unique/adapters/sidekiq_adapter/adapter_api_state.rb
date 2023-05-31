require 'active_support/concern'
require 'active_job/base'

module ActiveJob
  module Unique
    module Adapters
      module SidekiqAdapter
        module AdapterApiState
          extend ActiveSupport::Concern

          module ClassMethods
            def get_progress_stage_state(state_key, state_field)
              Sidekiq.redis_pool.with do |conn|
                conn.hget(state_key, state_field)
              end
            end

            def set_progress_stage_state(state_key, state_field, state_value)
              Sidekiq.redis_pool.with do |conn|
                conn.hset(state_key, state_field, state_value)
              end
            end

            def expire_progress_stage_state(state_key, state_field)
              Sidekiq.redis_pool.with do |conn|
                conn.hdel(state_key, state_field)
              end
            end

            def getset_progress_stage_state_flag(state_key, data)
              Sidekiq.redis_pool.with do |conn|
                conn.set(state_key, data, "GET")
              end
            end

            def get_progress_stage_state_flag(state_key)
              Sidekiq.redis_pool.with do |conn|
                conn.get(state_key)
              end
            end

            def set_progress_stage_state_flag(state_key, data)
              Sidekiq.redis_pool.with do |conn|
                conn.set(state_key, data)
              end
            end

            def expire_progress_stage_state_flag(state_key, seconds)
              Sidekiq.redis_pool.with do |conn|
                conn.expire(state_key, seconds)
              end
            end

          end
          # end ClassMethods

        end
      end
    end
  end
end
