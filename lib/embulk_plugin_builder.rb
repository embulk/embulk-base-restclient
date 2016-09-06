require 'embulk'
require 'embulk/version'

module EmbulkPluginBuilder
  class Runner
    def initialize
    end

    def run(argv)
      require 'java'
      require 'optparse'
      require 'embulk_plugin_builder/version'

      op = OptionParser.new
      op.version = EmbulkPluginBuilder::VERSION
      op.banner = "Usage: embulk_plugin_builder <config.yml>"

      require 'yaml'
      gen_plugin(YAML.load_file(argv[0]))
    end

    def gen_plugin(config={})
      language = config["language"]
      if language != "java"
        raise ArgumentError.new("Not supported: #{language}")
      end

      category = config["category"]
      if category != "web_api_input"
        raise ArgumentError.new("Not supported: #{category}")
      end

      require 'fileutils'
      require 'embulk_plugin_builder/data/package_data'

      embulk_category = category
      embulk_category = "input" if category == "web_api_input"

      name = config["name"].gsub(/[^a-zA-Z0-9_]+/, '_') # replace '-' to '_'

      full_project_name = "embulk-#{embulk_category}-#{name}"
      plugin_dir = "lib/embulk"
      plugin_path = "#{plugin_dir}/#{embulk_category}/#{name}.rb"

      if File.exist?(full_project_name)
        raise "./#{full_project_name} already exists. Please delete it first."
      end
      FileUtils.mkdir_p(full_project_name)

      puts "Creating #{full_project_name}/"

      success = false
      begin
        #
        # Generate gemspec
        #
        author = `git config user.name`.strip rescue ""
        author = "YOUR_NAME" if author.empty?
        email = `git config user.email`.strip rescue ""
        email = "YOUR_NAME" if email.empty?

        # variables used in erb templates
        ruby_class_name = name.split('_').map {|a| a.capitalize }.join
        java_iface_name = category.to_s.split('_').map {|a| a.capitalize }.join
        java_class_name = name.split('_').map {|a| a.capitalize }.join + java_iface_name + "Plugin"
        java_options_class_name = "PluginTask"
        java_options_definitions = gen_java_option_definitions(config)
        java_package_name = "org.embulk.#{embulk_category}.#{name}"
        display_name = name.split('_').map {|a| a.capitalize }.join(' ')
        display_category = category.to_s.gsub('_', ' ')

        extra_guess_erb = {}

        case category
        when "web_api_input"
          description = %[Loads records from #{display_name}.]
        end

        #
        # Generate project repository
        #
        pkg = EmbulkPluginBuilder::PackageData.new("new", full_project_name, binding())
        pkg.cp_erb("README.md.erb", "README.md")
        pkg.cp("LICENSE.txt", "LICENSE.txt")
        pkg.cp_erb("gitignore.erb", ".gitignore")

        pkg.cp("java/gradle/wrapper/gradle-wrapper.jar", "gradle/wrapper/gradle-wrapper.jar")
        pkg.cp("java/gradle/wrapper/gradle-wrapper.properties", "gradle/wrapper/gradle-wrapper.properties")
        pkg.cp("java/gradlew.bat", "gradlew.bat")
        pkg.cp("java/gradlew", "gradlew")
        pkg.set_executable("gradlew")
        pkg.cp("java/config/checkstyle/checkstyle.xml","config/checkstyle/checkstyle.xml")
        pkg.cp("java/config/checkstyle/default.xml","config/checkstyle/default.xml")
        pkg.cp_erb("java/build.gradle.erb", "build.gradle")
        pkg.cp_erb("java/plugin_loader.rb.erb", plugin_path)
        pkg.cp_erb("java/#{category}.java.erb", "src/main/java/#{java_package_name.gsub(/\./, '/')}/#{java_class_name}.java")
        pkg.cp_erb("java/#{category}_options.java.erb", "src/main/java/#{java_package_name.gsub(/\./, '/')}/#{java_options_class_name}.java")
        pkg.cp_erb("java/test.java.erb", "src/test/java/#{java_package_name.gsub(/\./, '/')}/Test#{java_class_name}.java")

        extra_guess_erb.each_pair do |erb,dest|
          pkg.cp_erb(erb, dest)
        end

        puts ""
        puts "Plugin template is successfully generated."
        puts "Next steps:"
        puts ""
        puts "  $ cd #{full_project_name}"
        puts "  $ ./gradlew package"
        puts ""

        success = true
      ensure
        Files.rm_rf full_project_name unless success
      end
    end

    def gen_java_option_definitions(config)
      body = ''
      config["options"].each do |k,v|
        option_name = k
        option_type = v['type']
        option_required = v['required']

        body << "    @Config(\"#{option_name}\")\n"
        body << "    @ConfigDefault(\"null\")\n" if !option_required
        body << "    public #{gen_java_type(option_type, option_required)} get#{option_name.split('_').map {|a| a.capitalize }.join}();\n"
        body << "\n"
      end

      #  apikey: {type: string, required: true}
      #  password: {type: string, required: true}
      #  target: {type: enum, required: true, association: targets}

      body
    end

    def gen_java_type(type, required)
      java_type = case (type)
      when "string"
        "String"
      when "int", "long"
        "long"
      when "float", "double"
        "double"
      when "boolean"
        "boolean"
      when "enum"
        "String"
      else
        raise ArgumentError.new("Not supported: #{type}")
      end

      !required ? "Optional<#{java_type.capitalize}>" : java_type
    end
  end

  def self.usage(message)
    STDERR.puts "embulk_plugin_builder v#{EmbulkPluginBuilder::VERSION}"
    STDERR.puts "Usage: embulk_plugin_builder <category> <name>"
    if message
      system_exit "error: #{message}"
    else
      system_exit "Use \`<command> --help\` to see description of the commands."
    end
  end

  def self.system_exit(message=nil)
    STDERR.puts message if message
    raise SystemExit.new(1, message)
  end

  def self.system_exit_success
    raise SystemExit.new(0)
  end

  def self.lib_path(path)
    path = '' if path == '/'
    jar, resource = __FILE__.split("!", 2)
    if resource
      lib = resource.split("/")[0..-2].join("/")
      "#{jar}!#{lib}/#{path}"
    elsif __FILE__ =~ /^(?:classpath|uri:classloader):/
      lib = __FILE__.split("/")[0..-2].join("/")
      "#{lib}/#{path}"
    else
      lib = File.expand_path File.dirname(__FILE__)
      File.join(lib, *path.split("/"))
    end
  end
end
