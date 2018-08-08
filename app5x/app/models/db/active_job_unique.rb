class Db::ActiveJobUnique < ApplicationRecord
  def self.find_one_or_create_by!(attributes, unique_attributes = nil)
    instance = find_by(unique_attributes || attributes)

    if instance.blank?
      begin
        if unique_attributes.present?
          attributes.symbolize_keys!
          unique_attributes.symbolize_keys!

          # merge unique_attributes and attributes
          additional_keys = unique_attributes.keys - attributes.keys
          attributes.merge!(unique_attributes.slice(*additional_keys)) if additional_keys.present?
        end

        instance = create!(attributes)
      rescue ActiveRecord::RecordNotUnique
        instance = find_by(unique_attributes || attributes)
      end
    end

    raise 'Instance object is missing...' if instance.blank?

    instance
  end
end
