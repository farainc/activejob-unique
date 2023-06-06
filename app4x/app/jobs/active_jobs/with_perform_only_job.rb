class ActiveJobs::WithPerformOnlyJob < ApplicationJob
  include ActiveJobs::JobStats

  queue_as :with_perform_only
  unique_for :while_executing, debug: true

  def perform(job_args, *_args)
    run(job_args, 300)

    true
  end
end
