require 'thread'
require 'wyrm/logger'

require 'wyrm/module'
require 'wyrm/pump'
require 'wyrm/pump_maker'
require 'wyrm/schema_tools'
require 'wyrm/core_extensions'

module Wyrm
  # This bypasses the need to marshal objects between two pumps.
  # It uses a queue of the record arrays instead.
  class Hole
    include PumpMaker
    include Logger

    # stateless module methods
    module QueueCodec
      def self.encode( obj, io_queue )
        io_queue.enq obj
      end

      def self.decode( io_queue, &block )
        obj = io_queue.deq
        yield obj if block_given?
        obj
      end
    end

    # This pretends to be just enough of an IO that we can use a queue to
    # connect a dump to a restore.
    #
    # Named for the mouth of a wormhole. Cos finding a good name for it is hard.
    class Mouth
      DEFAULT_QUEUE_SIZE = 5000

      def initialize( queue_size: DEFAULT_QUEUE_SIZE)
        raise '>= ruby-2.3.0 needed because we use Queue#close' if RUBY_VERSION < '2.3.0'
        @queue_size = queue_size
      end

      #############
      # interface for Hole
      def reset
        if @queue
          @queue.close.clear
          @queue = nil
        end
      end

      # queue could be empty while producer is generating something,
      # use a SizedQueue so we don't run out of memory during a big transfer
      def queue
        @queue ||= SizedQueue.new @queue_size
      end

      ##########
      # interface for codec
      def enq( value ); queue.enq value end

      def deq
        queue.deq or raise StopIteration
      rescue StopIteration
        raise "nil from deq, but queue not empty" unless queue.empty?
        raise
      end

      ##############
      # interface for Pump

      # eof after flush has been called and queue is empty.
      def eof?; queue.closed? && queue.empty? end

      # this gets called by pump after dump is finished
      def flush; queue.close end
    end

    def initialize( src_db, dst_db, drop_tables: true, queue_size: Mouth::DEFAULT_QUEUE_SIZE )
      # called only once per run, so not really a performance issue
      @drop_tables = drop_tables
      @queue_size = queue_size

      @src_db = maybe_deebe src_db
      @dst_db = maybe_deebe dst_db

      @src_db.extension :schema_dumper
    end

    attr_reader :src_db, :dst_db, :options, :queue_size
    def drop_tables?; @drop_tables end

    def mouth
      @mouth ||= Mouth.new queue_size: queue_size
    end

    def pump_options
      {io: mouth, codec: QueueCodec, logger: logger}
    end

    def src_pump
      @src_pump ||= Pump.new db: src_db, **pump_options
    end

    def dst_pump
      @dst_pump ||= Pump.new db: dst_db, **pump_options
    end

    def transfer_table( table_name )
      src_pump.table_name = dst_pump.table_name = table_name

      if src_pump.table_dataset.empty?
        logger.info "No records in #{table_name}"
        return
      end

      # Use threads so the db read/writes aren't waiting for one another.
      recv_thread = Thread.new{ dst_pump.restore }
      send_thread = Thread.new{ src_pump.dump }

      send_thread.join
      recv_thread.join
    ensure
      mouth.reset
      src_pump.table_name = dst_pump.table_name = nil
    end

    include SchemaTools

    def transfer_schema( &transfer_table_block )
      create_tables
      yield self if block_given? # transfer tables here
      create_indexes
    end

    def transfer_tables
      logger.info "transferring tables"
      src_db.tables.each {|table_name| transfer_table table_name }
    end

    def call
      if drop_tables?
        logger.info "dropping tables"
        drop_tables src_db.tables
      end
      transfer_schema { transfer_tables }
    end
  end
end
