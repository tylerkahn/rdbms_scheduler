require_relative './engine'
require_relative './task'
require 'forwardable'

module RdbmsScheduler
  class Client
    extend Forwardable

    attr_accessor :engine

    def initialize(*args)
      @engine = Engine.new(*args)
    end

    def_delegators :@engine, :add, :update_stales!, :table, :db, :disconnect!

    def poll(limit = nil, update_stales: true)
      TaskCollection.new(engine, engine.poll(limit, update_stales: update_stales))
    end

  end
end