class ActiveJobs::WithEnqueueOnlyJob < ApplicationJob
  include ActiveJobs::JobStats

  queue_as :with_enqueue_only
  unique_for :until_executing, true

  def perform(job_args, *_args)
    run(job_args)

    true
  end
end
