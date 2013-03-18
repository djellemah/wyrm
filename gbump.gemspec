# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'gbump/version'

Gem::Specification.new do |spec|
  spec.name          = "gbump"
  spec.version       = Gbump::VERSION
  spec.authors       = ["John Anderson"]
  spec.email         = ["panic@semiosix.com"]
  spec.description   = %q{Transfer from one SQL database to another}
  spec.summary       = %q{Transfer from one SQL database to another}
  spec.homepage      = "http://djellemah.com"
  spec.license       = "MIT"

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.3"
  spec.add_development_dependency "rake"
end
