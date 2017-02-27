package org.embulk.util.retryhelper.jettyclient92;

public interface JettyClient92ResponseReader<T>
{
    T readResponse(org.eclipse.jetty.client.api.Response response);
}
