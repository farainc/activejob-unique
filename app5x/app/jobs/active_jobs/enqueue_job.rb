class ActiveJobs::EnqueueJob < ApplicationJob
  queue_as :default

  def perform(klass_name, args_list)
    klass = klass_name.constantize

    args_list.each do |args|
      klass.perform_later(args)
    end

    true
  end
end
