$LOAD_PATH.push File.expand_path("../lib", __FILE__)
require 'embulk-input-web_api/version'

Gem::Specification.new do |gem|
  gem.name          = "embulk-input-web_api"
  gem.version       = Embulk::Input::WebApi::VERSION

  gem.summary       = "Embulk input plugin abstraction for loading records from Web API"
  gem.description   = "Embulk input plugin abstraction for loading records from Web API"
  gem.authors       = ["Muga Nishizawa"]
  gem.email         = ["muga.nishizawa@gmail.com"]
  gem.license       = "Apache 2.0"
  gem.homepage      = "https://github.com/muga/embulk-input-web_api"

  provided_classpath = Dir["classpath/jruby-complete-*.jar"] + Dir["classpath/icu4j-*.jar"]
  gem.files         = `git ls-files`.split("\n") + Dir["classpath/*.jar"] - provided_classpath
  gem.test_files    = gem.files.grep(%r"^(test|spec)/")
  gem.executables   = gem.files.grep(%r"^bin/").map{ |f| File.basename(f) }
  gem.require_paths = ["lib"]
  gem.has_rdoc      = false

  if RUBY_PLATFORM =~ /java/i
    gem.add_dependency "bundler", '>= 1.10.6'
    gem.add_dependency "msgpack", '~> 0.7.3'
    gem.add_dependency "liquid", '~> 3.0.6'

    # For embulk/guess/charset.rb. See also embulk-core/build.gradle
    gem.add_dependency "rjack-icu", '~> 4.54.1.1'

    gem.platform = 'java'

  else
    gem.add_dependency "jruby-jars", '= 9.1.2.0'
  end

  gem.add_development_dependency "embulk", [">= 0.8.8"]
  gem.add_development_dependency "rake", [">= 0.10.0"]
  gem.add_development_dependency "test-unit", ["~> 3.0.9"]
  gem.add_development_dependency "yard", ["~> 0.8.7"]
  gem.add_development_dependency "kramdown", ["~> 1.5.0"]
end
