workers Integer(ENV.fetch('PUMA_WORKER', 1))
threads_min = Integer(ENV.fetch('PUMA_THREADS_MIN', 1))
threads_max = Integer(ENV.fetch('PUMA_THREADS_MAX', 1))
threads threads_min, threads_max

environment ENV.fetch('RAILS_ENV') { ENV.fetch('RACK_ENV', 'development') }

plugin :tmp_restart

rackup 'config/sidekiq-web.ru'

port 5680
