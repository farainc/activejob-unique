class ActiveJobs::WithoutUniqueJob < ApplicationJob
  include ActiveJobs::JobStats

  queue_as :without_unique

  def perform(job_args, *_args)
    run(job_args)

    true
  end
end
