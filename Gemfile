source 'https://rubygems.org'

raise "You need >= ruby-2.3 for wyrm" unless RUBY_VERSION >= '2.3.0'

# Specify your gem's dependencies in wyrm.gemspec
gemspec

if Pathname('/usr/include/mysql').exist?
  # version is for mysql streaming result sets
  gem "mysql2", '>= 0.3.12'
end
