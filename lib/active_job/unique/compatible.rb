require 'active_job/base'

module ActiveJob
  module Unique
    module Compatible
      extend ActiveSupport::Concern

      included do
        # compatible with rails 4.x
        attr_writer   :priority
        attr_accessor :provider_job_id
        attr_accessor :executions
      end

      module ClassMethods
        def deserialize(job_data)
          begin
            job = super
          rescue LoadError
            sleep(1)

            job = super
          end

          job_uniqueness_mode = job_data['uniqueness_mode'] || uniqueness_mode

          if uniqueness_api.valid_uniqueness_mode?(job_uniqueness_mode)
            job.uniqueness_mode       = job_uniqueness_mode
            job.uniqueness_id         = job_data['uniqueness_id']
            job.uniqueness_skipped    = job_data['uniqueness_skipped']
            job.uniqueness_expires    = job_data['uniqueness_expires']
            job.uniqueness_expiration = job_data['uniqueness_expiration']
            job.uniqueness_debug      = job_data['uniqueness_debug'] || uniqueness_debug
          end

          job
        end
      end

      def serialize
        data = super

        if uniqueness_api.valid_uniqueness_mode?(uniqueness_mode.to_s.to_sym)
          data['uniqueness_id']         = uniqueness_id
          data['uniqueness_skipped']    = uniqueness_skipped
          data['uniqueness_expires']    = uniqueness_expires
          data['uniqueness_expiration'] = uniqueness_expiration
          data['uniqueness_mode']       = uniqueness_mode
          data['uniqueness_debug']      = uniqueness_debug
        end

        data
      end
    end
  end
end
