package org.embulk.util.retryhelper.jettyclient92;

public interface JettyClient92ClientCreator
{
    public org.eclipse.jetty.client.HttpClient create();
}
