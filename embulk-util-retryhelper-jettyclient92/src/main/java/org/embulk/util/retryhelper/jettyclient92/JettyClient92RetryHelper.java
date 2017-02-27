package org.embulk.util.retryhelper.jettyclient92;

import java.util.Locale;

import com.google.common.base.Throwables;

import org.embulk.spi.Exec;
import org.embulk.spi.util.RetryExecutor;

public class JettyClient92RetryHelper
        implements AutoCloseable
{
    public JettyClient92RetryHelper(int maximumRetries,
                            int initialRetryIntervalMillis,
                            int maximumRetryIntervalMillis,
                            JettyClient92ClientCreator clientCreator)
    {
        this(maximumRetries,
             initialRetryIntervalMillis,
             maximumRetryIntervalMillis,
             clientCreator.create(),
             true,
             Exec.getLogger(JettyClient92RetryHelper.class));
    }

    public JettyClient92RetryHelper(int maximumRetries,
                            int initialRetryIntervalMillis,
                            int maximumRetryIntervalMillis,
                            JettyClient92ClientCreator clientCreator,
                            final org.slf4j.Logger logger)
    {
        this(maximumRetries,
             initialRetryIntervalMillis,
             maximumRetryIntervalMillis,
             clientCreator.create(),
             true,
             logger);
    }

    public static JettyClient92RetryHelper createWithReadyMadeClient(int maximumRetries,
                                                             int initialRetryIntervalMillis,
                                                             int maximumRetryIntervalMillis,
                                                             final org.eclipse.jetty.client.HttpClient client,
                                                             final org.slf4j.Logger logger)
    {
        return new JettyClient92RetryHelper(maximumRetries,
                                    initialRetryIntervalMillis,
                                    maximumRetryIntervalMillis,
                                    client,
                                    false,
                                    logger);
    }

    private JettyClient92RetryHelper(int maximumRetries,
                             int initialRetryIntervalMillis,
                             int maximumRetryIntervalMillis,
                             final org.eclipse.jetty.client.HttpClient client,
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

    public <T> T requestWithRetry(final JettyClient92ResponseReader<T> responseReader,
                                  final JettyClient92SingleRequester singleRequester)
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
                            org.eclipse.jetty.client.api.Response response = singleRequester.requestOnce(client);

                            if (response.getStatus() / 100 != 2) {
                                throw new org.eclipse.jetty.client.HttpResponseException(response.getReason(), response);
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
            try {
                this.client.stop();
            }
            catch (Exception ex) {
                throw Throwables.propagate(ex);
            }
        }
    }

    private final int maximumRetries;
    private final int initialRetryIntervalMillis;
    private final int maximumRetryIntervalMillis;
    private final org.eclipse.jetty.client.HttpClient client;
    private final org.slf4j.Logger logger;
    private final boolean closeAutomatically;
}
