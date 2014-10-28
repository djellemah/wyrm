def from_gemrc
  # auto-load from ~/.gemrc
  home_gemrc = Pathname('~/.gemrc').expand_path

  if home_gemrc.exist?
    require 'yaml'
    # use all the sources specified in .gemrc
    YAML.load_file(home_gemrc)[:sources]
  end
end

# Use the gemrc source if defined, unless CANON is set,
# otherwise just use the default.
def preferred_sources
  rv = from_gemrc unless eval(ENV['CANON']||'')
  rv ||= []
  rv << 'http://rubygems.org' if rv.empty?
  rv
end

preferred_sources.each{|src| source src}

# Specify your gem's dependencies in wyrm.gemspec
gemspec

if Pathname('/usr/include/mysql').exist?
  # version is for mysql streaming result sets
  gem "mysql2", '>= 0.3.12'
end
