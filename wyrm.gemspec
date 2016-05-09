# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'wyrm/version'

Gem::Specification.new do |spec|
  spec.name          = "wyrm"
  spec.version       = Wyrm::VERSION
  spec.authors       = ["John Anderson"]
  spec.email         = ["panic@semiosix.com"]
  spec.description   = %q{Compressed cross-rdbms data transfer}
  spec.summary       = %q{Transfer from one SQL rdbms to another }
  spec.homepage      = "https://github.com/djellemah/wyrm"
  spec.license       = "MIT"

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  # need this version because clause_sql was moved to _insert_sql, used by pump
  spec.add_runtime_dependency 'sequel', '>= 4.10.0'

  spec.add_development_dependency "pry"
  spec.add_development_dependency "pry-debundle"
  spec.add_development_dependency "bundler", ">= 1.3"
  spec.add_development_dependency "rake"
  spec.add_development_dependency "rspec"
end
