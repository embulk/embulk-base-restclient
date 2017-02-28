package org.embulk.util.retryhelper.jetty92;

import org.eclipse.jetty.client.HttpClient;

public interface Jetty92ClientCreator
{
    HttpClient createAndStart() throws Exception;
}
