class Transferer
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

  def self.transfer( src_db, dst_db )
    new( src_db, dst_db ).transfer
  end
end
