require 'sequel'
require 'sqlite3'
require 'pathname'
require 'wyrm/dump_schema.rb'

include Wyrm

# pump = Wyrm::Pump.new db, :positions, codec: :yaml
dumper = DumpSchema.new db, '/tmp/test', pump: lambda{|_| Pump.new db, nil, codec: :yaml}
dumper = DumpSchema.new db, '/tmp/test', pump: ->(dump_schema){ Pump.new dump_schema.src_db, nil, codec: :yaml}
dumper.dump_tables

