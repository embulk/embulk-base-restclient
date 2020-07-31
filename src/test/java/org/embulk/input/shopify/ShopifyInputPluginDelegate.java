package org.embulk.input.shopify;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Strings;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.client.Client;
import javax.ws.rs.core.Response;
import javax.xml.bind.DatatypeConverter;
import org.embulk.base.restclient.DefaultServiceDataSplitter;
import org.embulk.base.restclient.RestClientInputPluginDelegate;
import org.embulk.base.restclient.RestClientInputTaskBase;
import org.embulk.base.restclient.ServiceDataSplitter;
import org.embulk.base.restclient.jackson.JacksonServiceRecord;
import org.embulk.base.restclient.jackson.JacksonServiceResponseMapper;
import org.embulk.base.restclient.jackson.StringJsonParser;
import org.embulk.base.restclient.record.RecordImporter;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.TaskReport;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;
import org.embulk.util.retryhelper.jaxrs.JAXRSClientCreator;
import org.embulk.util.retryhelper.jaxrs.JAXRSRetryHelper;
import org.embulk.util.retryhelper.jaxrs.JAXRSSingleRequester;
import org.embulk.util.retryhelper.jaxrs.StringJAXRSResponseEntityReader;
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.slf4j.Logger;

public class ShopifyInputPluginDelegate implements RestClientInputPluginDelegate<ShopifyInputPluginDelegate.PluginTask> {
    public interface PluginTask extends RestClientInputTaskBase {
        // client retry setting
        @Config("retry_limit")
        @ConfigDefault("7")
        public int getRetryLimit();

        @Config("initial_retry_wait")
        @ConfigDefault("1000")
        public int getInitialRetryWait();

        @Config("max_retry_wait")
        @ConfigDefault("60000")
        public int getMaxRetryWait();

        // An example required configuration

        // client timeout and connection setting: for RESTEasy
        @Config("connection_checkout_timeout")
        @ConfigDefault("30000")
        public long getConnectionCheckoutTimeout(); // millis

        @Config("establish_connection_timeout")
        @ConfigDefault("30000")
        public long getEstablishCheckoutTimeout(); // millis

        @Config("socket_timeout")
        @ConfigDefault("60000")
        public long getSocketTimeout(); // millis

        @Config("connection_pool_size")
        @ConfigDefault("8")
        public int getConnectionPoolSize();

        @Config("apikey")
        public String getApiKey();

        @Config("password")
        public String getPassword();

        @Config("store_name")
        public String getStoreName();
    }

    @Override  // Overridden from |InputTaskValidatable|
    public void validateInputTask(final PluginTask task) {
        if (Strings.isNullOrEmpty(task.getApiKey())) {
            throw new ConfigException("'apikey' must not be null or empty string.");
        }

        if (Strings.isNullOrEmpty(task.getPassword())) {
            throw new ConfigException("'password' must not be null or empty string.");
        }

        if (Strings.isNullOrEmpty(task.getStoreName())) {
            throw new ConfigException("'store_name' must not be null or empty string.");
        }
    }

    @Override  // Overridden from |ServiceResponseMapperBuildable|
    public JacksonServiceResponseMapper buildServiceResponseMapper(final PluginTask task) {
        return JacksonServiceResponseMapper.builder()
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

    @Override  // Overridden from |ConfigDiffBuildable|
    public ConfigDiff buildConfigDiff(
            final PluginTask task, final Schema schema, final int taskCount, final List<TaskReport> taskReports) {
        // should implement for incremental data loading
        return Exec.newConfigDiff();
    }

    @Override  // Overridden from |ServiceDataIngestable|
    public TaskReport ingestServiceData(
            final PluginTask task, final RecordImporter recordImporter, final int taskIndex, final PageBuilder pageBuilder) {
        try (final JAXRSRetryHelper retryHelper = new JAXRSRetryHelper(
                task.getRetryLimit(),
                task.getInitialRetryWait(),
                task.getMaxRetryWait(),
                new JAXRSClientCreator() {
                    @Override
                    public Client create() {
                        return ((ResteasyClientBuilder) ResteasyClientBuilder.newBuilder())
                            .connectionCheckoutTimeout(task.getConnectionCheckoutTimeout(), TimeUnit.MILLISECONDS)
                            .establishConnectionTimeout(task.getEstablishCheckoutTimeout(), TimeUnit.MILLISECONDS)
                            .socketTimeout(task.getSocketTimeout(), TimeUnit.MILLISECONDS)
                            .connectionPoolSize(task.getConnectionPoolSize())
                            .build();
                    }
                })) {
            int pageIndex = 1;
            while (true) {
                final String content = fetchFromShopify(retryHelper, task, pageIndex);
                final ArrayNode records = extractArrayField(content);

                int count = 0;
                for (final JsonNode record : records) {
                    if (!record.isObject()) {
                        logger.warn(String.format(Locale.ENGLISH, "A record must be Json object: %s", record.toString()));
                        continue;
                    }

                    try {
                        recordImporter.importRecord(new JacksonServiceRecord((ObjectNode) record), pageBuilder);
                    } catch (final Exception e) {
                        logger.warn(String.format(Locale.ENGLISH, "Skipped json: %s", record.toString()), e);
                    }
                    count++;
                }

                if (count == 0) {
                    break;
                }
                pageIndex++;
            }
        }
        return Exec.newTaskReport();
    }

    @Override  // Overridden from |ServiceDataSplitterBuildable|
    public ServiceDataSplitter<PluginTask> buildServiceDataSplitter(final PluginTask task) {
        return new DefaultServiceDataSplitter<PluginTask>();
    }

    private ArrayNode extractArrayField(final String content) {
        final ObjectNode jsonObject = jsonParser.parseJsonObject(content);
        final JsonNode jn = jsonObject.get("customers");
        if (jn.isArray()) {
            return (ArrayNode) jn;
        } else {
            throw new DataException("Expected array node: " + jsonObject.toString());
        }
    }

    private String fetchFromShopify(final JAXRSRetryHelper retryHelper, final PluginTask task, final int pageIndex) {
        return retryHelper.requestWithRetry(
            new StringJAXRSResponseEntityReader(),
            new JAXRSSingleRequester() {
                @Override
                public Response requestOnce(final Client client) {
                    final String url = String.format(
                            Locale.ENGLISH, "https://%s.myshopify.com/admin/customers.json", task.getStoreName());
                    final String userpass = String.format(Locale.ENGLISH, "%s:%s", task.getApiKey(), task.getPassword());

                    return client
                            .target(url)
                            .queryParam("page", pageIndex)
                            .queryParam("limit", PAGE_LIMIT)
                            .request()
                            .header("AUTHORIZATION", "Basic " + DatatypeConverter.printBase64Binary(userpass.getBytes()))
                            .get();
                }

                @Override
                public boolean isResponseStatusToRetry(final Response response) {
                    final int status = response.getStatus();
                    if (status == 429) {
                        return true;  // Retry if 429.
                    }
                    return status / 100 != 4;  // Retry unless 4xx except for 429.
                }
            });
    }

    private final Logger logger = Exec.getLogger(ShopifyInputPluginDelegate.class);

    private static final int PAGE_LIMIT = 250;

    private final StringJsonParser jsonParser = new StringJsonParser();
}
