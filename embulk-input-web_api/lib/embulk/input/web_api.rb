Embulk::JavaPlugin.register_input(
  :web_api, "org.embulk.input.web_api.WebApiInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))

