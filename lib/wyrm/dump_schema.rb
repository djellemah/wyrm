# Dump a schema and compressed data from a db to a set of files
#  src_db = Sequel.connect "postgres://localhost:5454/lots"
#  ds = DumpSchema.new src_db, Pathname('/var/data/lots')
#  ds.dump_schema
#  ds.dump_tables
class DumpSchema
  def initialize( src_db, container = nil, options = {} )
    @options = {:codec => :marshal}.merge( options )

    @src_db = src_db
    @container = Pathname(container)
  end

  attr_reader :src_db, :container, :codec

  def schema_migration
    @schema_migration ||= src_db.dump_schema_migration(:indexes=>false, :same_db => same_db)
  end

  def index_migration
    @index_migration ||= src_db.dump_indexes_migration(:same_db => same_db)
  end

  def fk_migration
    @fk_migration ||= src_db.dump_foreign_key_migration(:same_db => same_db)
  end

  def restore_migration
    <<-EOF
      require 'restore_migration'
      Sequel.migration do
        def db_pump
        end

        up do
          restore_tables
        end

        down do
          # from each table clear table
          each_table do |table_name|
            db_pump.restore table_name, io: io, db: db
          end
        end
      end
    EOF
  end

  def same_db
    false
  end

  def logger
    @logger ||= Logger.new STDERR
  end

  def dump_schema
    (container + '001_schema.rb').open('w') do |io|
      io.write schema_migration
    end

    (container + '002_populate_tables.rb').open('w') do |io|
      io.write restore_migration
    end

    (container + '003_indexes.rb').open('w') do |io|
      io.write index_migration
    end

    (container + '004_foreign keys.rb').open('w') do |io|
      io.write fk_migration
    end
  end

  def dump_one_table( table_name, pathname, db_pump )
    logger.info "dumping #{table_name} to #{pathname}"
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

    # generate the dump
    db_pump.dump table_name, db: src_db, io: zio

    # signal the copier thread to stop
    zio.close_write
    logger.debug 'finished dumping'
    # wait for copier thread to
    copier.join
    logger.debug 'stream copy thread finished'
  ensure
    zio.close unless zio.closed?
    fio.close unless fio.closed?
  end

  def dump_tables
    db_pump = DbPump.new( @options[:codec] )

    src_db.tables.each do |table_name|
      filename = container + "#{table_name}.dbp.bz2"
      dump_one_table table_name, filename, db_pump
    end
  end
end
