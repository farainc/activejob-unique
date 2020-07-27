if Redis.VERSION < 4.2
  module RedisCompatible
    extend ActiveSupport::Concern

    alias_method :exists?, :exists
  end

  Redis.senc(include: RedisCompatible)
end