require 'pathname'

require 'wyrm/module'
require 'wyrm/pump_maker'
require 'wyrm/schema_tools'
require 'wyrm/logger'

# Dump a schema and compressed data from a db to a set of files
#  src_db = Sequel.connect "postgres://localhost:5454/lots"
#  ds = DumpSchema.new src_db, Pathname('/var/data/lots')
#  ds.call
# TODO possibly use Gem::Package::TarWriter to write tar files
class Wyrm::Dump
  include Wyrm::PumpMaker
  include Wyrm::SchemaTools
  include Wyrm::Logger

  def initialize( src_db, container = nil, pump: nil )
    @container = Pathname.new container || '.'
    raise "#{@container} does not exist" unless @container.exist?

    @src_db = maybe_deebe src_db
    @pump = make_pump( @src_db, pump )

    @src_db.extension :schema_dumper
  end

  attr_reader :src_db, :container, :pump

  def same_db; false end

  def numbering
    @numbering ||= '000'
  end

  def dump_schema
    (container + "#{numbering.next!}_schema.rb").open('w') do |io|
      io.write schema_migration
    end
  end

  def dump_indexes
    (container + "#{numbering.next!}_indexes.rb").open('w') do |io|
      io.write index_migration
    end

    (container + "#{numbering.next!}_foreign_keys.rb").open('w') do |io|
      io.write fk_migration
    end
  end

  def write_through_bz2( pathname )
    fio = pathname.open('w')
    # open subprocess in read-write mode
    zio = IO.popen( "pbzip2 -z", 'r+' )
    copier = Thread.new do
      begin
        IO.copy_stream zio, fio
        logger.debug "finished stream copy"
      ensure
        fio.close
      end
    end

    yield zio

    # signal the copier thread to stop
    zio.close_write
    logger.debug 'finished dumping'

    # wait for copier thread to finish
    copier.join
    logger.debug 'stream copy thread finished'
  ensure
    zio.close unless zio.closed?
    fio.close unless fio.closed?
  end

  def dump_table( table_name, &io_block )
    pump.table_name = table_name
    if pump.table_dataset.empty?
      logger.info "No records in #{table_name}"
      return
    end

    filename = container + "#{table_name}.dbp.bz2"
    logger.info "dumping #{table_name} to #{filename}"

    write_through_bz2 filename do |zio|
      # generate the dump
      pump.io = zio
      pump.dump
    end
  rescue
    logger.error "failed dumping #{table_name}: #{$!.message}"
  end

  def dump_tables
    src_db.tables.each do |table_name|
      dump_table table_name
    end
  end

  def call
    dump_schema
    dump_tables
    dump_indexes
  end
end
