class ActiveJobs::WithoutUniqueJob < ApplicationJob
  include ActiveJobs::JobStats

  queue_as :without_unique

  def perform(args, *_args)
    run(args)

    true
  end
end
