Embulk::JavaPlugin.register_input(
  "example", "org.embulk.input.example.ExampleInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
