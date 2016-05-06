require 'wyrm/module'

module Wyrm
  def self.sanity_check_dcmp
    bzip_cmd = Wyrm::STREAM_DCMP.split(' ').first
    rv = `which #{bzip_cmd}`
    unless $?.exitstatus == 0
      puts "\n#{cmd} not installed or not in PATH"
      exit(1)
    end
  end
end
