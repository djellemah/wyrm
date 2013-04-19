# There are actually 2 sources for this:
# one is the src db, the other is the dumped files
# And the one that transfers live is another version
class Schema
  def initialize( src_db, dst_db = nil )
    @src_db = src_db
    @dst_db = dst_db
  end

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

  attr_accessor :dst_db
  attr_reader :src_db

  def same_db
    @dst_db.andand.database_type == @src_db.andand.database_type
  end

  def logger
    @logger ||= Logger.new STDERR
  end

  # create the destination schema
  def create
    eval( schema_migration ).apply dst_db, :up
  end

  # create indexes and foreign keys, and reset sequences
  def index
    logger.info "creating indexes"
    eval( index_migration ).apply dst_db, :up
    logger.info "creating foreign keys"
    eval( fk_migration ).apply dst_db, :up

    if dst_db.database_type == :postgres
      logger.info "reset primary key sequences"
      dst_db.tables.each{|t| dst_db.reset_primary_key_sequence(t)}
      logger.info "Primary key sequences reset successfully"
    end
  end

  def transfer_table( table_name, options = {} )
    options = OpenStruct.new( {page_size: 10000, dry_run: false}.merge( options ) )
    total_records = @src_db[table_name].count
    logger.info "transferring #{total_records}"
    column_names = @src_db.schema(table_name.to_sym).map( &:first )

    @src_db[table_name].each_page(options.page_size) do |page|
      logger.info "#{page.sql} of #{total_records}"
      unless options.dry_run
        @dst_db.transaction do
          rows_ary = []
          page.each do |row_hash|
            rows_ary << row_hash.values
          end
          @dst_db[table_name.to_sym].import column_names, rows_ary
        end
      end
    end
  end

  # copy the data in the tables
  def transfer
    create
    transfer_tables
    index
  end

  def dump_schema( container, options = {codec: :marshal} )
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

  def load_migrations( container )
    @schema_migration = (container + '001_schema.rb').read
    @index_migration = (container + '003_indexes.rb').read
    @fk_migration = (container + '004_foreign keys.rb').read
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

  def dump_tables( container, options = {:codec => :marshal} )
    container = Pathname(container)
    db_pump = DbPump.new( options[:codec] )

    src_db.tables.each do |table_name|
      filename = container + "#{table_name}.dbp.bz2"
      dump_one_table table_name, filename, db_pump
    end
  end

  def restore_one_table( table_file, db_pump )
    logger.info "restoring from #{table_file}"
    table_name = table_file.basename.sub_ext('').sub_ext('').to_s.to_sym
    # check if table has been restored already, and has the correct rows,
    # otherwise pass in a start row.
    db_pump.from_bz2 table_file, dst_db, table_name
  end

  def restore_tables( container, options = {:codec => :marshal} )
    db_pump = DbPump.new( options[:codec] )
    table_files = Pathname.glob Pathname(container) + '*dbp.bz2'
    table_files.sort_by{|tf| tf.stat.size}.each{|table_file| restore_one_table table_file, db_pump}
  end

  def self.transfer( src_db, dst_db )
    new( src_db, dst_db ).transfer
  end
end
