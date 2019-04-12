require 'active_support/concern'
require 'active_job/base'

module ActiveJob
  module Unique
    module Adapters
      module UniquenessAdapter
        module AdapterApiState
          extend ActiveSupport::Concern

          module ClassMethods
            def get_progress_stage_state(state_key, state_field)
            end

            def set_progress_stage_state(state_key, state_field, state_value)
            end

            def expire_progress_stage_state(state_key, state_field)
            end

            def getset_progress_stage_state_flag(state_key, data)
            end

            def get_progress_stage_state_flag(state_key)
            end

            def set_progress_stage_state_flag(state_key, data)
            end

            def expire_progress_stage_state_flag(state_key, seconds)
            end

          end
          # end ClassMethods

        end
      end
    end
  end
end
