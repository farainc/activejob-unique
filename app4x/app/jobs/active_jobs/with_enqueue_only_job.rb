class ActiveJobs::WithEnqueueOnlyJob < ApplicationJob
  include ActiveJobs::JobStats

  queue_as :with_enqueue_only
  unique_for :until_executing, true

  def perform(args, *_args)
    run(args)

    true
  end
end
