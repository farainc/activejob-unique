class CreateDbActiveJobUniques < ActiveRecord::Migration[5.1]
  def change
    create_table :active_job_uniques do |t|
      t.string  :job_name, limit: 64
      t.integer :args
      t.integer :around_enqueue, null: false, default: 0
      t.integer :around_perform, null: false, default: 0
      t.integer :performed, null: false, default: 0

      t.timestamps
    end

    add_index :active_job_uniques, [:job_name, :args], unique: true
  end
end
