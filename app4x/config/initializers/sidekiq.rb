Sidekiq.configure_server do |config|
  config.redis = { url: "redis://#{ENV['DC_REDIS_HOST'] || '127.0.0.1'}:6379" }
end

Sidekiq.configure_client do |config|
  config.redis = { url: "redis://#{ENV['DC_REDIS_HOST'] || '127.0.0.1'}:6379" }
end
