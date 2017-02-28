package org.embulk.util.retryhelper.jetty93;

import org.eclipse.jetty.client.HttpClient;

public interface Jetty93ClientCreator
{
    HttpClient createAndStart() throws Exception;
}
