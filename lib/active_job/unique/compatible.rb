require 'active_job/base'

module ActiveJob
  module Unique
    module Compatible
      extend ActiveSupport::Concern

      included do
        #compatible with rails 4.x
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

          job_uniqueness_mode = job_data['uniqueness_mode'] || self.uniqueness_mode

          if valid_uniqueness_mode?(job_uniqueness_mode)
            job.uniqueness_mode       = job_uniqueness_mode
            job.uniqueness_id         = job_data['uniqueness_id']
            job.uniqueness_skipped    = job_data['uniqueness_skipped']
            job.uniqueness_expires    = job_data['uniqueness_expires']
            job.uniqueness_expiration = job_data['uniqueness_expiration']
            job.uniqueness_debug      = job_data['uniqueness_debug'] || self.uniqueness_debug
          end

          job
        end
      end

      def serialize
        data = super

        if valid_uniqueness_mode?(self.uniqueness_mode.to_s.to_sym)
          data["uniqueness_id"]         = self.uniqueness_id
          data["uniqueness_skipped"]    = self.uniqueness_skipped
          data["uniqueness_expires"]    = self.uniqueness_expires
          data["uniqueness_expiration"] = self.uniqueness_expiration
          data["uniqueness_mode"]       = self.uniqueness_mode
          data["uniqueness_debug"]      = self.uniqueness_debug
        end

        data
      end

    end
  end
end
