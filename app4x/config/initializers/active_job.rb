require 'activejob-unique'

ActiveJob::Base.queue_name_prefix = 'fk'
ActiveJob::Base.queue_adapter = :sidekiq

ActiveJob::Unique::Stats.timezone = 'Pacific Time (US & Canada)'
