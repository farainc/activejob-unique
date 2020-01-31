$active_job_shutdown_pending = false

Sidekiq.configure_server do |config|
  config.redis = { url: 'redis://127.0.0.1:6379' }

  config.on(:quiet) do
    puts "Got TSTP, stopping further job processing..."

    $active_job_shutdown_pending = true
  end

  config.on(:shutdown) do
    puts "Got TERM, shutting down process..."
  end
end

Sidekiq.configure_client do |config|
  config.redis = { url: 'redis://127.0.0.1:6379' }
end


# some_job.rb
# def perform
#   # This job might process 1000s of items and take an hour.
#   # Have each iteration check for shutdown. big_list_of_items
#   # should only return unprocessed items so the loop will continue
#   # where it left off.
#   big_list_of_items.find_each do |item|
#     process(item)
#     # Sidekiq Pro will restart job immediately on process restart
#     raise Sidekiq::Shutdown if $shutdown_pending
#   end
# end