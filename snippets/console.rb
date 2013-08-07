require 'sequel'
require 'sqlite3'
require 'pathname'
require 'wyrm/dump_schema.rb'

# pump = DbPump.new db, :positions, codec: :yaml
dumper = DumpSchema.new db, '/tmp/test', pump: lambda{|_| DbPump.new db, nil, codec: :yaml}
dumper = DumpSchema.new db, '/tmp/test', pump: ->(dump_schema){ DbPump.new dump_schema.src_db, nil, codec: :yaml}
dumper.dump_tables

