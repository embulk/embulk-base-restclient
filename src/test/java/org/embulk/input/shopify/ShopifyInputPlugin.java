package org.embulk.input.shopify;

import java.util.concurrent.TimeUnit;

import javax.ws.rs.core.Response;
import javax.xml.bind.DatatypeConverter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.embulk.config.Config;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.spi.DataException;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.type.Types;

import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;

import org.embulk.base.restclient.RestClientInputPluginBase;
import org.embulk.base.restclient.RestClientInputTaskBase;
import org.embulk.base.restclient.ServiceResponseSchemaBuildable;
import org.embulk.base.restclient.ClientCreatable;
import org.embulk.base.restclient.JacksonServiceResponseSchema;
import org.embulk.base.restclient.json.StringJsonParser;
import org.embulk.base.restclient.record.JacksonServiceRecord;
import org.embulk.base.restclient.record.JacksonValueLocator;
import org.embulk.base.restclient.request.SingleRequester;
import org.embulk.base.restclient.request.RetryHelper;
import org.embulk.base.restclient.writer.SchemaWriter;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Locale.ENGLISH;
import static org.embulk.spi.Exec.newConfigDiff;

public class ShopifyInputPlugin
        extends RestClientInputPluginBase<ShopifyInputPlugin.PluginTask>
{
    public ShopifyInputPlugin()
    {
        super(new ClientCreator(), new ShopifyResponseSchemaBuilder());
    }

    public interface PluginTask
            extends RestClientInputTaskBase
    {
        // An example required configuration

        @Config("apikey")
        public String getApiKey();

        @Config("password")
        public String getPassword();

        @Config("store_name")
        public String getStoreName();
    }

    private final StringJsonParser jsonParser = new StringJsonParser();

    @Override
    public PluginTask validatePluginTask(PluginTask task)
    {
        if (isNullOrEmpty(task.getApiKey())) {
            throw new ConfigException("'apikey' must not be null or empty string.");
        }

        if (isNullOrEmpty(task.getPassword())) {
            throw new ConfigException("'password' must not be null or empty string.");
        }

        if (isNullOrEmpty(task.getStoreName())) {
            throw new ConfigException("'store_name' must not be null or empty string.");
        }
        return task;
    }

    @Override
    public Class<PluginTask> getInputTaskClass()
    {
        return PluginTask.class;
    }

    private static class ShopifyResponseSchemaBuilder
            implements ServiceResponseSchemaBuildable<JacksonValueLocator>
    {
        @Override  // Overridden from |ServiceResponseSchemaBuildable|
        public JacksonServiceResponseSchema buildServiceResponseSchema()
        {
            return JacksonServiceResponseSchema.builder()
                .add("id", Types.LONG)
                .add("email", Types.STRING)
                .add("accepts_marketing", Types.BOOLEAN)
                .add("created_at", Types.TIMESTAMP, "%Y-%m-%dT%H:%M:%S%z")
                .add("updated_at", Types.TIMESTAMP, "%Y-%m-%dT%H:%M:%S%z")
                .add("first_name", Types.STRING)
                .add("last_name", Types.STRING)
                .add("orders_count", Types.LONG)
                .add("state", Types.STRING)
                .add("total_spent", Types.STRING)
                .add("last_order_id", Types.LONG)
                .add("note", Types.STRING)
                .add("verified_email", Types.BOOLEAN)
                .add("multipass_identifier", Types.STRING)
                .add("tax_exempt", Types.BOOLEAN)
                .add("tags", Types.STRING)
                .add("last_order_name", Types.STRING)
                .add("default_address", Types.JSON)
                .add("addresses", Types.JSON)
                .build();
        }
    }

    @Override
    public ConfigDiff buildConfigDiff(PluginTask task)
    {
        // should implement for incremental data loading
        return newConfigDiff();
    }

    private static final int PAGE_LIMIT = 250;

    @Override
    protected void load(PluginTask task, RetryHelper client, SchemaWriter<JacksonValueLocator> schemaWriter, int taskCount, PageBuilder to)
    {
        int pageIndex = 1;

        while (true) {
            String content = fetchFromWebApi(task, client, pageIndex);
            ArrayNode records = extractArrayField(content);

            int count = 0;
            for (JsonNode record : records) {
                if (!record.isObject()) {
                    log.warn(String.format(ENGLISH, "A record must be Json object: %s", record.toString()));
                    continue;
                }

                try {
                    schemaWriter.addRecordTo(new JacksonServiceRecord((ObjectNode) record), to);
                }
                catch (Exception e) {
                    log.warn(String.format(ENGLISH, "Skipped json: %s", record.toString()), e);
                }
                count++;
            }

            if (count == 0) {
                break;
            }

            pageIndex++;
        }
    }

    private ArrayNode extractArrayField(String content)
    {
        ObjectNode jsonObject = jsonParser.parseJsonObject(content);
        JsonNode jn = jsonObject.get("customers");
        if (jn.isArray()) {
            return (ArrayNode) jn;
        }
        else {
            throw new DataException("Expected array node: " + jsonObject.toString());
        }
    }

    private String fetchFromWebApi(final PluginTask task, final RetryHelper retryHelper, final int pageIndex)
    {
        Response response = retryHelper.requestWithRetry(new SingleRequester() {
            @Override
            public Response requestOnce(javax.ws.rs.client.Client client)
            {
                final String url = String.format(ENGLISH, "https://%s.myshopify.com/admin/customers.json", task.getStoreName());
                final String userpass = String.format(ENGLISH, "%s:%s", task.getApiKey(), task.getPassword());

                return client
                        .target(url)
                        .queryParam("page", pageIndex)
                        .queryParam("limit", PAGE_LIMIT)
                        .request()
                        .header("AUTHORIZATION", "Basic " + DatatypeConverter.printBase64Binary(userpass.getBytes()))
                        .get();
            }

            @Override
            public boolean isResponseStatusToRetry(Response response)
            {
                int status = response.getStatus();
                if (status == 429) {
                    return true;
                }
                return status / 100 != 4;
            }
        });
        return response.readEntity(String.class);
    }

    private static class ClientCreator
            implements ClientCreatable<PluginTask>
    {
        @Override
        public javax.ws.rs.client.Client createClient(PluginTask task)
        {
            javax.ws.rs.client.Client client =
                ((ResteasyClientBuilder) ResteasyClientBuilder.newBuilder())
                .connectionCheckoutTimeout(task.getConnectionCheckoutTimeout(), TimeUnit.MILLISECONDS)
                .establishConnectionTimeout(task.getEstablishCheckoutTimeout(), TimeUnit.MILLISECONDS)
                .socketTimeout(task.getSocketTimeout(), TimeUnit.MILLISECONDS)
                .connectionPoolSize(task.getConnectionPoolSize())
                .build();
            return client;
        }
    }
}
