# Wyrm [![Gem Version](https://badge.fury.io/rb/wyrm.png)](http://badge.fury.io/rb/wyrm)

<!---
[![Build Status](https://travis-ci.org/djellemah/wyrm.png?branch=master)](https://travis-ci.org/djellemah/wyrm)
-->

Transfer a database (or single tables) from one rdbms to another (eg mysql to
postgres). Either via a set of files, or direct from one db server to another.

Has been used to dump > 100M dbs, and one 850G db.
Should theoretically work for any rdbms supported by [Sequel](http://sequel.jeremyevans.net/).

Dumps are (usually) compressed with bz2, using pbzip2. Fast *and* small :-D
For example: mysqldump | bzip2 for a certain 850G db comes to 127G. With wyrm
it comes to 134G.

Transfers tables and views only. Does not attempt to transfer
stored procs, permissions, triggers etc.

Handles tables with a single numeric key, single non-numeric key, and no
primary key. Haven't tried with compound primary key.

Depending on table keys will use different strategies to keep memory usage small.
Will use result set streaming if available.

Wyrm because:

- I like dragons
- I can have a Wyrm::Hole to transfer data O-;-)

## Dependencies

Ruby >= 2.3.0, for Queue#close

You must have a working
[pbzip2](http://compression.ca/pbzip2/ "Will use all your cores")
on your path. If you really have to use something else,
reassign ```Wyrm::STREAM_DCMP``` and ```Wyrm::STREAM_COMP``` .

## Installation

Add this line to your application's Gemfile:

    gem 'wyrm'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install wyrm

Make sure you install the db gems, typically

    $ gem install pg sequel_pg mysql2 sqlite3

## Usage

### CLI

Very basic cli at this point.

#### For direct db-to-db transfer

    $ wyrm mysql2://localhost/beeg_data_bays postgres://localhost/betta_dee_bee

#### Via files
From the source db to the file system

    $ wyrm mysql2://localhost/beeg_data_bays /tmp/lots_fs_space

Optionally transfer data. Already compressed, so no -z

    $ rsync -var /tmp/lots_fs_space user@host:/tmp/lots_fs_space

On the destination host

    $ wyrm /tmp/lots_fs_space postgres://localhost/betta_dee_bee

#### View contents of a dump file as yaml

    $ wyrm-view /tmp/lots_fs_space/some_table.dbp.bz2

### irb / pry

For restoring. dump will be similar.

``` ruby
require 'wyrm/restore_schema'

rs = Restore.new 'postgres://postgres@localhost/your_db', '/mnt/disk/wyrm'
rs.call
```

Directly transferring one table:

``` ruby
require 'sequel'
require 'wyrm/hole'

hole = Wyrm::Hole.new 'mysql2://localhost/beeg_data_bays', 'postgres://localhost/betta_dee_bee'

# transfer schema (no indexes), using Sequel::SchemaDumper extension, see
# Sequel::SchemaDumper#dump_schema_migration for options
table_schema = hole.src_db.dump_table_schema :the_stuff_you_want
Sequel.migration{ change{ eval table_schema } }.apply hole.dst_db, :up

hole.transfer_table :the_stuff_you_want

# it's just Sequel...
hole.dst_db[:the_stuff_you_want].where( some_thing: /a pattern/ ).limit(10).all
```

Get to the dumped rows:

``` ruby
require 'sequel' # for some demarshaling
require 'wyrm/pump'

dbp = Wyrm::Pump.new io: IO.popen('pbzip2 -d -c /mnt/disk/wyrm/things.dbp.bz2')
# each_row also returns an Enumerator if no block is given, similar to much
# ruby core stuff. Although it's not rewindable.
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
