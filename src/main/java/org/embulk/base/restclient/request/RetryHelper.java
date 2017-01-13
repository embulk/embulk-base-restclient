package org.embulk.base.restclient.request;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;

import org.embulk.spi.util.RetryExecutor.Retryable;
import org.embulk.spi.util.RetryExecutor.RetryGiveupException;

import org.slf4j.Logger;

import org.embulk.base.restclient.RestClientInputTaskBase;

import static com.google.common.base.Throwables.propagate;
import static java.util.Locale.ENGLISH;
import static org.embulk.spi.Exec.getLogger;
import static org.embulk.spi.util.RetryExecutor.retryExecutor;

public class RetryHelper<T extends RestClientInputTaskBase>
        implements AutoCloseable
{
    private final Logger log = getLogger(RetryHelper.class);
    protected final Client client;
    protected final T task;

    private RetryHelper(T task, Client client)
    {
        this.task = task;
        this.client = client;
    }

    public Client getClient()
    {
        return client;
    }

    public String fetchWithRetry(final SingleRequester call)
    {
        try {
            return retryExecutor()
                    .withRetryLimit(task.getRetryLimit())
                    .withInitialRetryWait(task.getInitialRetryWait())
                    .withMaxRetryWait(task.getMaxRetryWait())
                    .runInterruptible(new Retryable<String>() {
                        @Override
                        public String call()
                                throws Exception
                        {
                            // javax.ws.rs.ProcessingException happens by connect and read timed out
                            javax.ws.rs.core.Response response = call.request();

                            if (response.getStatus() / 100 != 2) {
                                throw new WebApplicationException(response);
                            }

                            // javax.ws.rs.ProcessingException happens by read timed out
                            return response.readEntity(String.class);
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
            Thread.currentThread().interrupt();
            throw propagate(e);
        }
        catch (RetryGiveupException e) {
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

    public static RetryHelper.Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private Client client;

        private Builder()
        {
        }

        public RetryHelper.Builder client(Client client)
        {
            this.client = client;
            return self();
        }

        private RetryHelper.Builder self()
        {
            return this;
        }

        public <T extends RestClientInputTaskBase> RetryHelper build(T task)
        {
            return new RetryHelper<>(task, client);
        }
    }
}
