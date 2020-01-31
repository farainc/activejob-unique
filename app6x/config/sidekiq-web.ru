# This file is used by Rack-based servers to start sidekiq web
# we load environment to get the rails environment

require 'sidekiq'
require_relative 'initializers/sidekiq'

require 'sidekiq/web'
require 'activejob-unique-web'


use Rack::Session::Cookie, key: 'sidekiq',
                           expire_after: 2_592_000,
                           secret: ENV['SECRET_KEY_BASE']
run Sidekiq::Web
