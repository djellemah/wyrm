# Wyrm

Transfer data from one database to another. Has been used to dump > 100M dbs,
and one 850G db. Should theoretically work for any dbs supported by Sequel.

Currently transfers tables and views only. Does not attempt to transfer
stored procs, permissions, triggers etc.

Works best for tables that have single numeric primary keys, but should also
handle compound primary keys and tables without primary keys.

Wyrm because:

- I like dragons
- I can have a Wyrm::Hole to transfer data through :-D

## Installation

Add this line to your application's Gemfile:

    gem 'wyrm'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install wyrm

Make sure you install the db gems, typically

    $ gem install pg mysql2

## Usage

This is mostly a toolkit right now. To transfer from mysql to postgres do:
```ruby
require 'sequel'
require 'pathname'

# on the source host
# dump tables from mysql
require 'wyrm/dump_schema'
src_db = Sequel.connect "mysql2://localhost/lots"
ds = DumpSchema.new src_db, Pathname('/tmp/lots')
ds.dump_schema

# this might take a while ;-)
ds.dump_tables

# transfer data. Already compressed, so no -z
# rsync -var /tmp/lots user@host:/var/data/

# on the destination host
# restore tables to postgres
require 'wyrm/restore_schema'
dst_db = Sequel.connect "postgres://localhost/lots"
rs = RestoreSchema.new dst_db, Pathname('/var/data/lots')
rs.create
rs.restore_tables
rs.index
```

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
