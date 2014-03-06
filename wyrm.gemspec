# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'wyrm/version'

Gem::Specification.new do |spec|
  spec.name          = "wyrm"
  spec.version       = Wyrm::VERSION
  spec.authors       = ["John Anderson"]
  spec.email         = ["panic@semiosix.com"]
  spec.description   = %q{Transfer from one SQL database to another}
  spec.summary       = %q{Transfer from one SQL database to another}
  spec.homepage      = "https://github.com/djellemah/wyrm"
  spec.license       = "MIT"

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_runtime_dependency 'sequel'
  spec.add_runtime_dependency "fastandand"

  spec.add_development_dependency "pry"
  spec.add_development_dependency "pry-debugger"
  spec.add_development_dependency "bundler", ">= 1.3"
  spec.add_development_dependency "rake"
  spec.add_development_dependency "rspec"
  spec.add_development_dependency "pg"
  spec.add_development_dependency "sequel_pg"
  spec.add_development_dependency "sqlite3"

  # version is for mysql streaming result sets
  spec.add_development_dependency "mysql2", '>= 0.3.12'
end
