require 'logger'
require 'wyrm/pump_maker'

# Load a schema from a set of dump files (from DumpSchema)
# and restore the table data
#  dst_db = Sequel.connect "postgres://localhost:5454/lots"
#  rs = RestoreSchema.new dst_db, Pathname('/var/data/lots')
#  rs.create
#  rs.restore_tables
class RestoreSchema
  include PumpMaker

  def initialize( dst_db, container, pump: nil )
    @container = Pathname.new container
    @dst_db = maybe_deebe dst_db
    @pump = make_pump( @dst_db, pump )
  end

  attr_reader :pump
  attr_reader :dst_db
  attr_reader :container

  # sequel wants migrations numbered, but it's a bit of an annoyance for this.
  def find_single( glob )
    candidates =Pathname.glob container + glob
    raise "too many #{candidates.inspect} for #{glob}" unless candidates.size == 1
    candidates.first
  end

  def schema_migration
    @schema_migration ||= find_single( '*schema.rb' ).read
  end

  def index_migration
    @index_migration ||= find_single( '*indexes.rb' ).read
  end

  def fk_migration
    @fk_migration ||= find_single( '*foreign_keys.rb' ).read
  end

  def reload_migrations
    @fk_migration = nil
    @index_migration = nil
    @schema_migration = nil
  end

  def logger
    @logger ||= Logger.new STDERR
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

  # create the destination schema
  def create
    logger.info "creating tables"
    eval( schema_migration ).apply dst_db, :up
  end

  # assume the table name is the base name of table_file
  def restore_table( table_file )
    logger.info "restoring from #{table_file}"
    pump.table_name = table_file.basename.sub_ext('').sub_ext('').to_s.to_sym
    # TODO check if table has been restored already, and has the correct rows,
    # otherwise pass in a start row.
    IO.popen( "pbzip2 -d -c #{table_file}" ) do |io|
      pump.io = io
      pump.restore
    end
  end

  def restore_tables
    table_files = Pathname.glob Pathname(container) + '*dbp.bz2'
    table_files.sort_by{|tf| tf.stat.size}.each{|table_file| restore_table table_file}
  end
end
