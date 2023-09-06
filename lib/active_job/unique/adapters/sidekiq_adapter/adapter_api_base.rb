require 'sidekiq'
require 'sidekiq/api'

module ActiveJob
  module Unique
    module Adapters
      module SidekiqAdapter
        module AdapterApiBase
          extend ActiveSupport::Concern

          module ClassMethods
            def another_job_in_queue?(job_name, queue_name, uniqueness_id)
              queue = Sidekiq::Queue.new(queue_name)
              return false if queue.size.zero?

              queue.any? { |job| job.args.any? { |j| j['job_class'] == job_name && j['uniqueness_id'] == uniqueness_id } }
            end

            def another_job_in_worker?(job_name, queue_name, uniqueness_id, job_id)
              Sidekiq::Workers.new.any? { |_p, _t, w| w['queue'] == queue_name && w['payload']['wrapped'] == job_name && w['payload']['args'].any? { |j| j['uniqueness_id'] == uniqueness_id && j['job_id'] != job_id } }
            end
          end
        end
      end
    end
  end
end
