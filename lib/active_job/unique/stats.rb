module ActiveJob
  module Unique
    class Stats
      @timezone = 'UTC'

      class << self
        attr_accessor :timezone
      end
    end
  end
end
