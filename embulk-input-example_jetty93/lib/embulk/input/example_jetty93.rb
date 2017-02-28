Embulk::JavaPlugin.register_input(
  "example_jetty93", "org.embulk.input.example_jetty93.ExampleJetty93InputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
