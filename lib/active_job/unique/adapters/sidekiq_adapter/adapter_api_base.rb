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
              Sidekiq::WorkSet.new.each do |_pid, _tid, w|
                next unless w['queue'] == queue_name
                next unless /"job_class":"#{job_name}".*"uniqueness_id":"#{uniqueness_id}"/i.match?(w['payload'])
                return true unless /"job_id":"#{job_id}"/i.match?(w['payload'])
              end

              false
            end
          end
        end
      end
    end
  end
end
