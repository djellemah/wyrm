require 'gbump/schema.rb'

# There are actually 2 sources for this:
# one is the src db, the other is the dumped files
# And the one that transfers live is another version
class FileSchema < Schema
  def initialize( container, dst_db )
    @container = container
    @dst_db = dst_db
    load_migrations @container
  end
end
