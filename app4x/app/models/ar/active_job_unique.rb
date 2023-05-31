class Ar::ActiveJobUnique < ApplicationRecord
  def self.find_one_or_create_by!(unique_or_additional_attributes, unique_attributes = nil, &block)
    raise "unique_or_additional_attributes can't be blank." if unique_or_additional_attributes.blank?

    attributes = unique_or_additional_attributes.symbolize_keys
    query_attributes = attributes

    if unique_attributes.present?
      unique_attributes = unique_attributes.symbolize_keys
      query_attributes = unique_attributes
    end

    instance = find_by(query_attributes)
    return instance if instance.present?

    attributes.merge!(unique_attributes || {})

    retry_times = 0

    begin
      instance = transaction(requires_new: true) { create!(attributes, &block) }
      instance.instance_variable_set(:@new_created, true)
    rescue ActiveRecord::RecordNotUnique
      instance = find_by(query_attributes)
      return instance if instance.present?

      raise 'Instance object is missing, and find_one_or_create_by! inside a transaction.' unless ActiveRecord::Base.connection.open_transactions.zero?
      raise 'Instance object is missing, and over retry limits （5 tiems).' if retry_times > 5

      retry_times += 1
      sleep(retry_times * 0.00025)
      retry
    end

    instance
  end
end
