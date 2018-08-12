require 'sidekiq'

require_relative 'api_base'
require_relative 'adapters/sidekiq_adapter/web'

module ActiveJob
  module Unique
    module Web
    end
  end
end
