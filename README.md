# Wyrm

Transfer data from one database to another. Has been used to dump > 100M dbs,
and one 850G db. Should theoretically work for any dbs supported by Sequel.

Currently transfers tables and views only. Does not attempt to transfer
stored procs, permissions, triggers etc.

Works best for tables that have single numeric primary keys, but should also
handle compound primary keys and tables without primary keys.

Wyrm because:

- I like dragons
- I can (eventually) have a Wyrm::Hole to transfer data through :-D

## Dependencies

You must have a working
[pbzip2](http://compression.ca/pbzip2/ "Will use all your cores")
on your path.

## Installation


Add this line to your application's Gemfile:

    gem 'wyrm'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install wyrm

Make sure you install the db gems, typically

    $ gem install pg sequel_pg mysql2

## Usage

### CLI

Very basic cli at this point.

From the source db to the file system

    $ wyrm mysql2://localhost/beeg_data_bays /tmp/lots_fs_space

Optionally transfer data. Already compressed, so no -z

    $ rsync -var /tmp/lots_fs_space user@host:/tmp/lots_fs_space

On the destination host

    $ wyrm /tmp/lots_fs_space postgres://localhost/betta_dee_bee

### irb / pry

For restoring. dump will be similar.

``` ruby
require 'wyrm/restore_schema'
rs = RestoreSchema.new 'postgres://postgres@localhost/your_db', '/mnt/disk/wyrm'
rs.create
rs.restore_tables
rs.index
```

Or for the lower-level stuff

``` ruby
require 'sequel'
require 'wyrm/db_pump'

db = Sequel.connect 'postgres://postgres@localhost/other_db'
dbp = DbPump.new db, :things
dbp.io = IO.popen "pbzip2 -d -c /mnt/disk/wyrm/things.dbp.bz2'
dbp.each_row do |row|
  puts row.inspect
end
```

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
