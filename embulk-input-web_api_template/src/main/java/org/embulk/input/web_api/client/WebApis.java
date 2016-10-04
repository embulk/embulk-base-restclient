package org.embulk.input.web_api.client;

import org.embulk.input.web_api.WebApiPluginTask;
import org.embulk.spi.Exec;
import org.embulk.spi.util.RetryExecutor.RetryGiveupException;
import org.embulk.spi.util.RetryExecutor.Retryable;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InterruptedIOException;

import static com.google.common.base.Throwables.propagate;
import static com.google.common.base.Throwables.propagateIfInstanceOf;
import static java.util.Locale.ENGLISH;
import static org.embulk.spi.util.RetryExecutor.retryExecutor;

public class WebApis
{
    private static final Logger log = Exec.getLogger(WebApis.class);

    public static <Response, PluginTask extends WebApiPluginTask> Response fetchWithRetry(
            final PluginTask task,
            final RetryableWebApiCall<PluginTask, Response> call)
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
                            return call.execute(task);
                        }

                        @Override
                        public boolean isRetryableException(Exception e)
                        {
                            if (call.isNotRetryableException(e)) {
                                return false;
                            }

                            if (e instanceof WebApiClientException &&
                                    call.isNotRetryableResponse(e)) {
                                return false;
                            }

                            return true;
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


}
