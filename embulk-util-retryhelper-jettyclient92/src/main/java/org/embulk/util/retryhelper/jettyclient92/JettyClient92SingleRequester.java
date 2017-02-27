package org.embulk.util.retryhelper.jettyclient92;

public abstract class JettyClient92SingleRequester
{
    public abstract org.eclipse.jetty.client.api.Response requestOnce(org.eclipse.jetty.client.HttpClient client) throws Exception;

    public final boolean toRetry(Exception exception) {
        // TODO

        // Expects |javax.ws.rs.WebApplicationException| is throws in case of HTTP error status
        // such as implemented in |JettyClient92RetryHelper|.
        if (exception instanceof org.eclipse.jetty.client.HttpResponseException) {
            return isResponseStatusToRetry(((org.eclipse.jetty.client.HttpResponseException) exception).getResponse());
        }
        else {
            return isExceptionToRetry(exception);
        }
    }

    protected abstract boolean isResponseStatusToRetry(org.eclipse.jetty.client.api.Response response);

    protected boolean isExceptionToRetry(Exception exception) {
        return false;
    }
}
