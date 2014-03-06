require 'fastandand'
Sequel.extension :migration
require 'wyrm/module'

# needs dst_db for mutate operations
# and src_db for fetch operations
module Wyrm::SchemaTools
  # some includers will need to provide a different implementation for this.
  def same_db
    respond_to?( :dst_db ) && respond_to?( :src_db ) && dst_db.andand.database_type == src_db.andand.database_type
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

  def drop_table_options
    @drop_table_options ||=
    begin
      if dst_db.opts[:adapter] == 'postgres'
        {cascade: true}
      else
        {}
      end
    end
  end

  # Delete given tables.
  # Recurse if there are foreign keys preventing table deletion.
  # This implementation will fail for tables with mutual foreign keys.
  # TODO maybe this should use the schema down migration?
  def drop_tables( tables )
    foreign_keyed_tables = []
    tables.each do |table_name|
      begin
        logger.debug "dropping #{table_name}"
        dst_db.drop_table? table_name, drop_table_options

      rescue Sequel::ForeignKeyConstraintViolation => ex
        foreign_keyed_tables << table_name

      rescue Sequel::DatabaseError => ex
        # Mysql2::Error: Cannot delete or update a parent row: a foreign key constraint fails
        if ex.message =~ /foreign key constraint fails/
          foreign_keyed_tables << table_name
        else
          raise
        end
      end
    end

    # this should be temporary
    if tables.sort == foreign_keyed_tables.sort
      raise "can't remove #{tables.inspect} because they have mutual foreign keys"
    end

    # recursively delete tables
    drop_tables foreign_keyed_tables.shuffle unless foreign_keyed_tables.empty?
  end

  def create_tables
    logger.info "creating tables"
    eval( schema_migration ).apply dst_db, :up
  end

  def create_indexes
    # create indexes and foreign keys, and reset sequences
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
end
