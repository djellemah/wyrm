require 'ostruct'
require 'pathname'

require 'wyrm/logger'
require 'wyrm/module'
require 'wyrm/pump_maker'
require 'wyrm/schema_tools'

# Load a schema from a set of dump files (from DumpSchema)
# and restore the table data.
#  dst_db = Sequel.connect "postgres://localhost:5454/lots"
#  rs = RestoreSchema.new dst_db, '/var/data/lots'
#  rs.call
# TODO the problem with lazy loading the schema files is that
# errors in indexes and foreign keys will only be picked up at the
# end of they probably lengthy table restore process.
# TODO check if table has been restored already, and has the correct rows,
class Wyrm::Restore
  include PumpMaker
  include SchemaTools
  include Wyrm::Logger

  def initialize( container, dst_db, pump: nil, drop_tables: false )
    @container = Pathname.new container
    @dst_db = maybe_deebe dst_db
    @pump = make_pump( @dst_db, pump )

    options.drop_tables = drop_tables
  end

  attr_reader :pump
  attr_reader :dst_db
  attr_reader :container

  def options
    @options ||= OpenStruct.new
  end

  # sequel wants migrations numbered, but it's a bit of an annoyance for this.
  def find_single( glob )
    candidates = Pathname.glob container + glob
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

  # assume the table name is the base name of table_file pathname
  def restore_table( table_file )
    logger.info "restoring from #{table_file}"
    pump.table_name = table_file.basename.sub_ext('').sub_ext('').to_s.to_sym
    open_bz2 table_file do |io|
      pump.io = io
      pump.restore filename: table_file
    end
  end

  # open a dbp.bz2 file and either yield or return an io of the uncompressed contents
  def open_bz2( table_name, &block )
    table_file =
    case table_name
    when Symbol
      container + "#{table_name}.dbp.bz2"
    when Pathname
      table_name
    else
      raise "Don't know what to do with #{table_name.inspect}"
    end

    IO.popen "pbzip2 -d -c #{table_file}", &block
  end

  def table_files
    Pathname.glob container + '*.dbp.bz2'
  end

  def restore_tables
    table_files.sort_by{|tf| tf.stat.size}.each{|table_file| restore_table table_file}
  end

  def table_names
    table_files.map do |path|
      path.basename.to_s.split(?.)[0...-2].last.to_sym
    end
  end

  def call
    drop_tables(table_names) if options.drop_tables
    create_tables
    restore_tables
    create_indexes
  end
end
