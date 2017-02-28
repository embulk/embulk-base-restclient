package org.embulk.util.retryhelper.jetty93;

import java.util.Locale;

import com.google.common.base.Throwables;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpResponseException;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import org.embulk.spi.Exec;
import org.embulk.spi.util.RetryExecutor;

public class Jetty93RetryHelper
        implements AutoCloseable
{
    public Jetty93RetryHelper(int maximumRetries,
                              int initialRetryIntervalMillis,
                              int maximumRetryIntervalMillis,
                              Jetty93ClientCreator clientCreator)
    {
        this.maximumRetries = maximumRetries;
        this.initialRetryIntervalMillis = initialRetryIntervalMillis;
        this.maximumRetryIntervalMillis = maximumRetryIntervalMillis;
        try {
            this.clientStarted = clientCreator.createAndStart();
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        this.closeAutomatically = true;
        this.logger = Exec.getLogger(Jetty93RetryHelper.class);
    }

    public Jetty93RetryHelper(int maximumRetries,
                              int initialRetryIntervalMillis,
                              int maximumRetryIntervalMillis,
                              Jetty93ClientCreator clientCreator,
                              final org.slf4j.Logger logger)
    {
        this.maximumRetries = maximumRetries;
        this.initialRetryIntervalMillis = initialRetryIntervalMillis;
        this.maximumRetryIntervalMillis = maximumRetryIntervalMillis;
        try {
            this.clientStarted = clientCreator.createAndStart();
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        this.closeAutomatically = true;
        this.logger = logger;
    }

    /**
     * Creates a {@code Jetty93RetryHelper} instance with a ready-made Jetty 9.3 {@code HttpClient} instance.
     *
     * Note that the {@code HttpClient} instance is not automatically closed.
     */
    public static Jetty93RetryHelper createWithReadyMadeClient(int maximumRetries,
                                                               int initialRetryIntervalMillis,
                                                               int maximumRetryIntervalMillis,
                                                               final org.eclipse.jetty.client.HttpClient clientStarted,
                                                               final org.slf4j.Logger logger)
    {
        return new Jetty93RetryHelper(maximumRetries,
                                      initialRetryIntervalMillis,
                                      maximumRetryIntervalMillis,
                                      clientStarted,
                                      false,
                                      logger);
    }

    private Jetty93RetryHelper(int maximumRetries,
                               int initialRetryIntervalMillis,
                               int maximumRetryIntervalMillis,
                               final org.eclipse.jetty.client.HttpClient clientStarted,
                               boolean closeAutomatically,
                               final org.slf4j.Logger logger)
    {
        this.maximumRetries = maximumRetries;
        this.initialRetryIntervalMillis = initialRetryIntervalMillis;
        this.maximumRetryIntervalMillis = maximumRetryIntervalMillis;
        this.logger = logger;
        this.clientStarted = clientStarted;
        this.closeAutomatically = closeAutomatically;
    }

    public <T> T requestWithRetry(final Jetty93ResponseReader<T> responseReader,
                                  final Jetty93SingleRequester singleRequester)
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
                            Response.Listener listener = responseReader.getListener();
                            singleRequester.requestOnce(clientStarted, listener);
                            Response response = responseReader.getResponse();
                            if (response.getStatus() / 100 != 2) {
                                throw new HttpResponseException("Response not 2xx: " + response.getReason(), response);
                            }
                            return responseReader.readResponseContent();
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
        if (this.closeAutomatically && this.clientStarted != null) {
            try {
                if (this.clientStarted.isStarted()) {
                    this.clientStarted.stop();
                }
            }
            catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            finally {
                this.clientStarted.destroy();
            }
        }
    }

    private final int maximumRetries;
    private final int initialRetryIntervalMillis;
    private final int maximumRetryIntervalMillis;
    private final org.eclipse.jetty.client.HttpClient clientStarted;
    private final org.slf4j.Logger logger;
    private final boolean closeAutomatically;
}
