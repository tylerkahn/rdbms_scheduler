module RdbmsScheduler
  class Task
    attr_accessor :engine, :token, :row, :id, :data

    def initialize(engine, token, id, data, row)
      @engine = engine
      @token = token
      @id = id
      @data = data && (data.is_a?(Hash) ? data : JSON.parse(data))
      @row = row
    end

    def finish!
      engine.finish!(id, token) > 0
    end

    def retry!
      engine.retry!(id, token) > 0
    end

  end

  class TaskCollection
    attr_accessor :engine, :tasks, :token

    def initialize(engine, rows)
      @engine = engine
      @rows = rows
      @tasks = []
      @token = nil
    end

    def acquire_all!
      [] if @rows.empty?
      token, acquired_rows = engine.acquire_all!(@rows.map {|x| x[:id]})
      tasks = acquired_rows.map do |row|
        Task.new(engine, token, row[:id], row[engine.col(:data)], row)
      end
    end
  end
end