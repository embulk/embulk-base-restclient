package org.embulk.base.restclient.request;

import java.util.Locale;

import com.google.common.base.Throwables;

import org.embulk.spi.Exec;
import org.embulk.spi.util.RetryExecutor;

import org.slf4j.Logger;

public class RetryHelper
{
    public RetryHelper(final javax.ws.rs.client.Client client,
                       final int retryLimit,
                       final int initialRetryWait,
                       final int maxRetryWait)
    {
        this.logger = Exec.getLogger(RetryHelper.class);
        this.client = client;
        this.retryLimit = retryLimit;
        this.initialRetryWait = initialRetryWait;
        this.maxRetryWait = maxRetryWait;
    }

    public <T> T requestWithRetry(final ResponseReadable<T> responseReader,
                                  final SingleRequester singleRequester)
    {
        try {
            return RetryExecutor
                .retryExecutor()
                .withRetryLimit(retryLimit)
                .withInitialRetryWait(initialRetryWait)
                .withMaxRetryWait(maxRetryWait)
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
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Throwables.propagate(e);
        }
        catch (RetryExecutor.RetryGiveupException e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    private final Logger logger;
    private final javax.ws.rs.client.Client client;
    private final int retryLimit;
    private final int initialRetryWait;
    private final int maxRetryWait;
}
