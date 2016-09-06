package org.embulk.input.web_api.client;

import org.embulk.input.web_api.WebApiPluginTask;
import org.embulk.spi.Exec;
import org.embulk.spi.util.RetryExecutor.RetryGiveupException;
import org.embulk.spi.util.RetryExecutor.Retryable;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Locale;

import static com.google.common.base.Throwables.propagate;
import static com.google.common.base.Throwables.propagateIfInstanceOf;
import static org.embulk.spi.util.RetryExecutor.retryExecutor;

public abstract class WebApiClient<PluginTask extends WebApiPluginTask>
{
    private final Logger log = Exec.getLogger(WebApiClient.class);

    public <Response> Response fetchWithRetry(final PluginTask task)
            throws IOException
    {
        try {
            return retryExecutor()
                    .withRetryLimit(task.getRetryLimit())
                    .withInitialRetryWait(task.getInitialRetryWait())
                    .withMaxRetryWait(task.getMaxRetryWait())
                    .runInterruptible(new Retryable<Response>() {

                        @Override
                        public Response call()
                                throws Exception
                        {
                            return fetch(task);
                        }

                        @Override
                        public boolean isRetryableException(Exception exception)
                        {
                            if (isNotRetryableNonWebApiClientException(exception)) {
                                return false;
                            }

                            if (exception instanceof WebApiClientException &&
                                    isNotRetryableWebApiCode(((WebApiClientException)exception).getCode())) {
                                return false;
                            }

                            return true;
                        }

                        @Override
                        public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait)
                                throws RetryGiveupException
                        {
                            String message = String.format(Locale.ENGLISH, "Retrying %d/%d after %d seconds. Message: %s",
                                    retryCount, retryLimit, retryWait / 1000, exception.getMessage());
                            if (retryCount % 3 == 0) {
                                log.warn(message, exception);
                            }
                            else {
                                log.warn(message);
                            }
                        }

                        @Override
                        public void onGiveup(Exception e, Exception e1)
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

    protected abstract <Response> Response fetch(PluginTask task);

    protected abstract boolean isNotRetryableNonWebApiClientException(Exception execption);

    protected abstract boolean isNotRetryableWebApiCode(int code);
}
