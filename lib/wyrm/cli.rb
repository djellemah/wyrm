module Wyrm
  def self.sanity_check_pbzip2
    rv = `which pbzip2`
    unless $?.exitstatus == 0
      puts "\npbzip2 not installed or not in PATH"
      exit(1)
    end
  end
end
