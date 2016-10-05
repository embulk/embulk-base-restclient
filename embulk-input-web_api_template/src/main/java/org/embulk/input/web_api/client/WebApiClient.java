package org.embulk.input.web_api.client;

import org.embulk.input.web_api.WebApiPluginTask;
import org.embulk.spi.util.RetryExecutor.RetryGiveupException;
import org.embulk.spi.util.RetryExecutor.Retryable;
import org.slf4j.Logger;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;

import java.io.IOException;
import java.io.InterruptedIOException;

import static com.google.common.base.Throwables.propagate;
import static com.google.common.base.Throwables.propagateIfInstanceOf;
import static java.util.Locale.ENGLISH;
import static org.embulk.spi.Exec.getLogger;
import static org.embulk.spi.util.RetryExecutor.retryExecutor;

public class WebApiClient
        implements AutoCloseable
{
    private static final Logger log = getLogger(WebApiClient.class);
    protected final Client client;

    private WebApiClient(Client client)
    {
        this.client = client;
    }

    public Client getClient()
    {
        return client;
    }

    public <TASK extends WebApiPluginTask, RESPONSE> RESPONSE fetchWithRetry(
            final TASK task,
            final RetryableWebApiCall<TASK, RESPONSE> call)
            throws IOException
    {
        try {
            return retryExecutor()
                    .withRetryLimit(task.getRetryLimit())
                    .withInitialRetryWait(task.getInitialRetryWait())
                    .withMaxRetryWait(task.getMaxRetryWait())
                    .runInterruptible(new Retryable<RESPONSE>() {

                        @Override
                        public RESPONSE call()
                                throws Exception
                        {
                            javax.ws.rs.core.Response response = call.request(task);

                            if (response.getStatus() / 100 != 2) {
                                throw new WebApplicationException(response);
                            }

                            return call.readResponse(response);
                        }

                        @Override
                        public boolean isRetryableException(Exception e)
                        {
                            return !call.isNotRetryable(e);
                        }

                        @Override
                        public void onRetry(Exception e, int retryCount, int retryLimit, int retryWait)
                                throws RetryGiveupException
                        {
                            String message = String.format(ENGLISH, "Retrying %d/%d after %d seconds. Message: %s",
                                    retryCount, retryLimit, retryWait / 1000, e.getMessage());
                            if (retryCount % 3 == 0) {
                                log.warn(message, e);
                            }
                            else {
                                log.warn(message);
                            }
                        }

                        @Override
                        public void onGiveup(Exception first, Exception last)
                                throws RetryGiveupException
                        {
                        }
                    });
        }
        catch (InterruptedException e) {
            throw new InterruptedIOException();
        }
        catch (RetryGiveupException e) {
            propagateIfInstanceOf(e.getCause(), IOException.class);
            throw propagate(e.getCause());
        }
    }

    @Override
    public void close()
    {
        if (client != null) {
            client.close();
        }
    }

    public static class Builder
    {
        private Client client;

        public Builder()
        {
        }

        public WebApiClient.Builder client(Client client)
        {
            this.client = client;
            return self();
        }

        private WebApiClient.Builder self()
        {
            return this;
        }

        public WebApiClient build()
        {
            return new WebApiClient(client);
        }
    }
}
