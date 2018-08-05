require_relative 'web/sidekiq_web'

module ActiveJob
  module Unique
    module Web
      extend ActiveSupport::Autoload

      autoload :SidekiqWeb
    end
  end
end
