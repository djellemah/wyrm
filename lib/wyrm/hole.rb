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

    # This is the codec. Named for the mouth of a wormhole. Cos finding a good name for this is hard.
    #
    # Connects the two pumps together. Implements Codec, Quacks like IO.
    class Mouth
      include Logger

      def initialize
        @flushed = false
      end

      # This is a bit weird because io_queue will usually == self
      def encode( obj, io_queue )
        io_queue.enq obj
      end

      # This is a bit weird because io_queue will usually == self
      def decode( io_queue, &block )
        obj = io_queue.deq
        yield obj if block_given?
        obj
      end

      def reset
        # do this first, so any (hopefully not) remaining waiters don't
        # go into the blocking deq again.
        @flushed = false

        # clear any poisons, and release any (hopefully not) remaining waiters
        queue.clear
      end

      # queue could be empty while producer is generating something,
      # so only eof after flush has been called.
      def eof?
        # queue is not empty if it's been poisoned.
        @flushed && queue.empty?
      end

      # use a SizedQueue so we don't run out of memory during a big transfer
      def queue
        @queue ||=
        if RUBY_VERSION == '2.1.0'
          raise "Queue broken in 2.1.0 possibly related to https://bugs.ruby-lang.org/issues/9302"
        else
          SizedQueue.new 5000
        end
      end

      def enq( value )
        queue.enq value
      end

      def deq( *args )
        rv = queue.deq( *args )
        if rv == :poison
          poison_queue
          raise StopIteration
        end
        rv
      end

      def poison_queue
        # poison the queue. waiters will have to re-queue this.
        queue << :poison if queue.empty? && queue.num_waiting > 0
      end

      # this gets called after dump is finished, by pump
      def flush
        # do this first, so any non-poisoned waiters will eof
        # synchronisation is not really important because
        @flushed = true
        poison_queue
      end
    end

    def initialize( src_db, dst_db, drop_tables: true, queue_size: 5000 )
      # called only once per run, so not really a performance issue
      @options = method(__method__).kwargs_as_hash( binding )

      @src_db = maybe_deebe src_db
      @dst_db = maybe_deebe dst_db

      @src_db.extension :schema_dumper
    end

    attr_reader :src_db, :dst_db, :options

    def mouth
      @mouth ||= Mouth.new
    end

    def src_pump
      @src_pump ||= Pump.new( {db: src_db, io: mouth, codec: mouth, logger: logger}.merge( options[:pump] ||{} ) )
    end

    def dst_pump
      @dst_pump ||= Pump.new( {db: dst_db, io: mouth, codec: mouth, logger: logger}.merge( options[:pump] ||{} ) )
    end

    def transfer_table( table_name )
      mouth.reset
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
    end

    include SchemaTools

    def transfer_schema( &transfer_table_block )
      create_tables

      # transfer tables here
      yield self if block_given?

      create_indexes
    end

    def transfer_tables
      logger.info "transferring tables"
      src_db.tables.each do |table_name|
        transfer_table table_name
      end
    end

    def call
      if options[:drop_tables]
        logger.info "dropping tables"
        drop_tables src_db.tables
      end

      transfer_schema do
        transfer_tables
      end
    end

  end
end
