package org.embulk.util.retryhelper.jaxrs;

import java.util.Locale;

import com.google.common.base.Throwables;

import org.embulk.spi.Exec;
import org.embulk.spi.util.RetryExecutor;

public class JAXRSRetryHelper
        implements AutoCloseable
{
    public JAXRSRetryHelper(int maximumRetries,
                            int initialRetryIntervalMillis,
                            int maximumRetryIntervalMillis,
                            JAXRSClientCreator clientCreator)
    {
        this(maximumRetries,
             initialRetryIntervalMillis,
             maximumRetryIntervalMillis,
             clientCreator.create(),
             true,
             Exec.getLogger(JAXRSRetryHelper.class));
    }

    public JAXRSRetryHelper(int maximumRetries,
                            int initialRetryIntervalMillis,
                            int maximumRetryIntervalMillis,
                            JAXRSClientCreator clientCreator,
                            final org.slf4j.Logger logger)
    {
        this(maximumRetries,
             initialRetryIntervalMillis,
             maximumRetryIntervalMillis,
             clientCreator.create(),
             true,
             logger);
    }

    /**
     * Creates a {@code JAXRSRetryHelper} instance with a ready-made JAX-RS {@code Client} instance.
     *
     * Note that the {@code Client} instance is not automatically closed.
     */
    public static JAXRSRetryHelper createWithReadyMadeClient(int maximumRetries,
                                                             int initialRetryIntervalMillis,
                                                             int maximumRetryIntervalMillis,
                                                             final javax.ws.rs.client.Client client,
                                                             final org.slf4j.Logger logger)
    {
        return new JAXRSRetryHelper(maximumRetries,
                                    initialRetryIntervalMillis,
                                    maximumRetryIntervalMillis,
                                    client,
                                    false,
                                    logger);
    }

    private JAXRSRetryHelper(int maximumRetries,
                             int initialRetryIntervalMillis,
                             int maximumRetryIntervalMillis,
                             final javax.ws.rs.client.Client client,
                             boolean closeAutomatically,
                             final org.slf4j.Logger logger)
    {
        this.maximumRetries = maximumRetries;
        this.initialRetryIntervalMillis = initialRetryIntervalMillis;
        this.maximumRetryIntervalMillis = maximumRetryIntervalMillis;
        this.client = client;;
        this.closeAutomatically = closeAutomatically;
        this.logger = logger;
    }

    public <T> T requestWithRetry(final JAXRSResponseReader<T> responseReader,
                                  final JAXRSSingleRequester singleRequester)
    {
        try {
            return RetryExecutor
                .retryExecutor()
                .withRetryLimit(this.maximumRetries)
                .withInitialRetryWait(this.initialRetryIntervalMillis)
                .withMaxRetryWait(this.maximumRetryIntervalMillis)
                .runInterruptible(new RetryExecutor.Retryable<T>() {
                        @Override
                        public T call()
                                throws Exception
                        {
                            // |javax.ws.rs.ProcessingException| can be throws
                            // by timeout in connection or reading.
                            javax.ws.rs.core.Response response = singleRequester.requestOnce(client);

                            if (response.getStatus() / 100 != 2) {
                                throw new javax.ws.rs.WebApplicationException(response);
                            }

                            return responseReader.readResponse(response);
                        }

                        @Override
                        public boolean isRetryableException(Exception exception)
                        {
                            return singleRequester.toRetry(exception);
                        }

                        @Override
                        public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait)
                                throws RetryExecutor.RetryGiveupException
                        {
                            String message = String.format(
                                Locale.ENGLISH, "Retrying %d/%d after %d seconds. Message: %s",
                                retryCount, retryLimit, retryWait / 1000, exception.getMessage());
                            if (retryCount % 3 == 0) {
                                logger.warn(message, exception);
                            }
                            else {
                                logger.warn(message);
                            }
                        }

                        @Override
                        public void onGiveup(Exception first, Exception last)
                                throws RetryExecutor.RetryGiveupException
                        {
                        }
                    });
        }
        catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw Throwables.propagate(ex);
        }
        catch (RetryExecutor.RetryGiveupException ex) {
            throw Throwables.propagate(ex.getCause());
        }
    }

    @Override
    public void close()
    {
        if (this.closeAutomatically && this.client != null) {
            this.client.close();
        }
    }

    private final int maximumRetries;
    private final int initialRetryIntervalMillis;
    private final int maximumRetryIntervalMillis;
    private final javax.ws.rs.client.Client client;
    private final org.slf4j.Logger logger;
    private final boolean closeAutomatically;
}
