require 'sidekiq'
require_relative 'sidekiq_web_api'
require_relative 'sidekiq/server'

module ActiveJob
  module Unique
    module Web
      module SidekiqWeb
        extend ActiveSupport::Autoload

        include ActiveJob::Unique::Web::SidekiqWebApi

        autoload :Server
      end
    end
  end
end
