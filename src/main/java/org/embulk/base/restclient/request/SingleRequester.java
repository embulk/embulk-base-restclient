package org.embulk.base.restclient.request;

/**
 * SingleRequester is to define a single request to the target REST service to be ready for retries.
 *
 * It is expected to use with {@link RetryHelper} as follows. {@code RetryHelper} is given for {@code ServiceDataIngestable#ingestServiceData}.
 *
 * <pre>{@code
 * javax.ws.rs.core.Response response = retryHelper.requestWithRetry(
 *     new StringResponseEntityReader(),
 *     new SingleRequester() {
 *         @Override
 *         public Response requestOnce(javax.ws.rs.client.Client client)
 *         {
 *             return client.target("https://example.com/api/resource").request().get;
 *         }
 *
 *         @Override
 *         public boolean isResponseStatusToRetry(javax.ws.rs.core.Response response)
 *         {
 *             return (response.getStatus() / 100) == 4;
 *         }
 *     });
 * }</pre>
 *
 * @see ResponseReadable
 * @see StringResponseEntityReader
 */
public abstract class SingleRequester
{
    /**
     * Requests to the target service with the given {@code javax.ws.rs.client.Client}.
     *
     * The method is {@code abstract} that must be overridden.
     */
    public abstract javax.ws.rs.core.Response requestOnce(javax.ws.rs.client.Client client);

    /**
     * Returns {@code true} if the given {@code Exception} from {@link RetryHelper} is to retry.
     *
     * This method cannot be overridden. Override {@code isResponseStatusRetryable} and {@code isExceptionRetryable}
     * instead. {@code isResponseStatusRetryable} is called for {@code javax.ws.rs.WebApplicationException}.
     * {@code isExceptionRetryable} is called for other exceptions.
     */
    public final boolean toRetry(Exception exception) {
        // Expects |javax.ws.rs.WebApplicationException| is throws in case of HTTP error status
        // such as implemented in |RetryHelper|.
        if (exception instanceof javax.ws.rs.WebApplicationException) {
            return isResponseStatusToRetry(((javax.ws.rs.WebApplicationException) exception).getResponse());
        }
        else {
            return isExceptionToRetry(exception);
        }
    }

    /**
     * Returns {@code true} if the given {@code javax.ws.rs.core.Response} is to retry.
     *
     * This method is {@code abstract} to be overridden, and {@code protected} to be called from {@code isRetryable}.
     */
    protected abstract boolean isResponseStatusToRetry(javax.ws.rs.core.Response response);

    /**
     * Returns true if the given {@code Exception} is to retry.
     *
     * This method available to override, and {@code protected} to be called from {@code isRetryable}.
     */
    protected boolean isExceptionToRetry(Exception exception) {
        return false;
    }
}
