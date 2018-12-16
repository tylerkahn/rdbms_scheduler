require 'sequel'
require 'securerandom'
require 'chrono'
require 'active_support/time_with_zone'
require 'active_support/core_ext/time/zones'

module RdbmsScheduler
  class Engine
    attr_accessor :db, :table

    def initialize(db_uri = nil, table_name = nil, create_alter_table: false, column_prefix: 'rdbms_scheduler_', logger: nil, config: nil)
      config ||= {}
      db_uri ||= config[:db_uri]
      @table_name = table_name || config[:table_name]
      @column_prefix = (column_prefix || config[:column_prefix]).to_s

      if db_uri.nil? || @table_name.nil?
        raise ArgumentError, "db_uri and table_name must not be nil"
      end

      @db = Sequel.connect(db_uri, logger: logger || config[:logger])

      create_alter_table! if create_alter_table

      @table = @db[table_name.to_sym]
    end

    def update_stales!
      token = generate_token
      stale_row_count = table.where(
        Sequel.lit("#{col(:next_run_at)} <= (#{now_epoch} - #{col(:max_stale_seconds)})")
      ).where(
        Sequel.lit("#{col(:lease_expires_at)} <= #{now_epoch}")
      ).update({
        col(:lease_expires_at) => Sequel.lit(now_epoch),
        col(:token) => token
      })

      if stale_row_count > 0
        table.where({col(:token) => token}).all.each do |row|
          run_time_offset_seconds = row[col(:run_time_offset_seconds)] || 0
          now = row[col(:time_zone)] ? Time.now.in_time_zone(row[col(:time_zone)]) : Time.now
          next_run_at = Chrono::NextTime.new(now: now - run_time_offset_seconds, source: row[col(:cron)]).to_time.to_i + run_time_offset_seconds
          table.where(id: row[:id]).update({
            col(:next_run_at) => next_run_at,
            col(:lease_expires_at) => 0
          })
        end
      end
      stale_row_count
    end

    def add(cron, lease_seconds = nil, **other_cols)
      now = Time.now
      # TODO: properly calculate runtime offset here
      next_run_time = Chrono::NextTime.new(now: now, source: cron).to_time 
      interval_seconds = Chrono::NextTime.new(now: next_run_time, source: cron).to_time - next_run_time
      data = other_cols[:data] && JSON.generate(other_cols[:data])
      other_cols.delete(:data)
      table.insert({
        col(:data) => data,
        col(:cron) => cron,
        col(:lease_seconds) => lease_seconds || (interval_seconds/2).to_i - 1,
        col(:max_stale_seconds) => interval_seconds.to_i - 1, 
        col(:next_run_at) => next_run_time.to_i + (other_cols[:run_time_offset_seconds] || 0),
        **Hash[other_cols.map {|k, v| [k.to_sym == :id ? :id : col(k), v]}],
      })
    end

    def poll(limit = nil, update_stales: true)
      if update_stales
        update_stales!
      end
      q = table.where(Sequel.lit(
        "#{col(:next_run_at)} <= #{now_epoch}"
      )).where(Sequel.lit(
        "#{col(:next_run_at)} > (#{now_epoch} - #{col(:max_stale_seconds)})"
      )).where(Sequel.lit(
        "#{col(:lease_expires_at)} <= #{now_epoch}"
      )).order(Sequel.asc(col(:next_run_at)))
      (limit ? q.limit(limit) : q).all
    end

    def acquire_all!(ids)
      token = generate_token
      table.where(id: ids)
        .where(Sequel.lit("#{col(:next_run_at)} <= #{now_epoch}"))
        .where(Sequel.lit("#{col(:next_run_at)} > (#{now_epoch} - #{col(:max_stale_seconds)} )"))
        .update({
          col(:lease_expires_at) => Sequel.lit("#{now_epoch} + #{col(:lease_seconds)}"),
          col(:token) => token
        })
      updated_rows = table.where({col(:token) => token}).where(Sequel.lit("#{now_epoch} < #{col(:lease_expires_at)}") )
      [token, updated_rows.all]
    end

    def finish!(id, token)
      if id.is_a?(Hash) && id.has_key?(col(:cron)) && id.has_key?(col(:run_time_offset_seconds)) && id.has_key?(col(:time_zone))
        cron = id[col(:cron)]
        run_time_offset_seconds = id[col(:run_time_offset_seconds)]
        time_zone = id[col(:time_zone)]
      else
        cron, run_time_offset_seconds, time_zone = table.where(id: id).get([col(:cron), col(:run_time_offset_seconds), col(:time_zone)])
      end
      run_time_offset_seconds ||= 0
      now = time_zone ? Time.now.in_time_zone(time_zone) : Time.now
      next_run_time = Chrono::NextTime.new(now: now - run_time_offset_seconds, source: cron).to_time.to_i + run_time_offset_seconds
      table.where(id: id, col(:token) => token)
        .where(Sequel.lit("#{now_epoch} < #{col(:lease_expires_at)}"))
        .update({
          col(:lease_expires_at) => 0,
          col(:next_run_at) => next_run_time
        })
    end

    def retry!(id, token)
      table.where(id: id, col(:token) => token)
        .where(Sequel.lit("#{now_epoch} < #{col(:lease_expires_at)}"))
        .update(col(:lease_expires_at) => 0) 
    end

    def col(c)
      "#{@column_prefix}#{c}".to_sym
    end

    private

    def now_epoch
      case db.database_type
      when :sqlite
        "(strftime('%s', 'now'))"
      when :mysql
        "(UNIX_TIMESTAMP())"
      when :postgres
        "(extract(epoch from now()))"
      else
        Time.now.to_i.to_s
      end
    end

    def generate_token
      SecureRandom.hex
    end

    def create_alter_table!
      cols = [
        ["primary_key", :id],
        ["String", col(:cron), {null: false}],
        ["String", col(:data)],
        ["Fixnum", col(:lease_seconds), {null: false}],
        ["String", col(:token), {index: true}],
        ["Fixnum", col(:max_stale_seconds), {null: false}],
        ["Fixnum", col(:lease_expires_at), {index: true, null: false, default: 0}],
        ["Fixnum", col(:next_run_at), {index: true, null: false}],
        ["Fixnum", col(:run_time_offset_seconds)],
        ["String", col(:time_zone)]
      ]

      table_name = @table_name.to_sym
      column_prefix = @column_prefix
      @db.create_table?(table_name) do
        cols.each do |col|
          send(*col)
        end
      end

      schema = @db.schema(table_name).to_h
      @db.alter_table(table_name) do
        primary_key :id unless schema.key?(:id)
        cols[1..-1].each do |col|
          send(:add_column, col[1], col[0], col[2] || {}) unless schema.key?(col[1])
        end
      end
    end
  end
end
