Embulk::JavaPlugin.register_output(
  "example", "org.embulk.output.example.ExampleOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
