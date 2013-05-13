require 'logger'

# Load a schema from a set of dump files (from DumpSchema)
# and restore the table data
#  dst_db = Sequel.connect "postgres://localhost:5454/lots"
#  rs = RestoreSchema.new dst_db, Pathname('/var/data/lots')
#  rs.create
#  rs.restore_tables
class RestoreSchema
  def initialize( dst_db, container )
    @container = container
    @dst_db = dst_db
    @options = {:codec => :marshal}
    load_migrations @container
  end

  attr_reader :dst_db

  def load_migrations( container )
    @schema_migration = (container + '001_schema.rb').read
    @index_migration = (container + '003_indexes.rb').read
    @fk_migration = (container + '004_foreign keys.rb').read
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
    eval( schema_migration ).apply dst_db, :up
  end

  def restore_one_table( table_file, db_pump )
    logger.info "restoring from #{table_file}"
    table_name = table_file.basename.sub_ext('').sub_ext('').to_s.to_sym
    # check if table has been restored already, and has the correct rows,
    # otherwise pass in a start row.
    db_pump.from_bz2 table_file, dst_db, table_name
  end

  def restore_tables
    db_pump = DbPump.new( options[:codec] )
    table_files = Pathname.glob Pathname(container) + '*dbp.bz2'
    table_files.sort_by{|tf| tf.stat.size}.each{|table_file| restore_one_table table_file, db_pump}
  end
end
