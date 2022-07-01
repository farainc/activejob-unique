# This file is auto-generated from the current state of the database. Instead
# of editing this file, please use the migrations feature of Active Record to
# incrementally modify your database, and then regenerate this schema definition.
#
# This file is the source Rails uses to define your schema when running `bin/rails
# db:schema:load`. When creating a new database, `bin/rails db:schema:load` tends to
# be faster and is potentially less error prone than running all of your
# migrations from scratch. Old migrations may fail to apply correctly if those
# migrations use external dependencies or application code.
#
# It's strongly recommended that you check this file into your version control system.

ActiveRecord::Schema[7.0].define(version: 2018_08_03_055220) do
  create_table "active_job_uniques", charset: "utf8mb4", force: :cascade do |t|
    t.string "job_name", limit: 64
    t.integer "job_args"
    t.integer "around_enqueue", default: 0, null: false
    t.integer "around_perform", default: 0, null: false
    t.integer "performed", default: 0, null: false
    t.datetime "created_at", precision: nil, null: false
    t.datetime "updated_at", precision: nil, null: false
    t.index ["job_name", "job_args"], name: "index_active_job_uniques_on_job_name_and_args", unique: true
  end

end
