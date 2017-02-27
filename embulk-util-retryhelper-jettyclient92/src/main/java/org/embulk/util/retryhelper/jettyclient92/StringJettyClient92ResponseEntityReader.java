package org.embulk.util.retryhelper.jettyclient92;

import org.eclipse.jetty.client.api.ContentResponse;

import java.nio.charset.StandardCharsets;

public class StringJettyClient92ResponseEntityReader
        implements JettyClient92ResponseReader<String>
{
    public final String readResponse(org.eclipse.jetty.client.api.Response response)
    {
        return new String(((ContentResponse) response).getContent(), StandardCharsets.UTF_8);
    }
}
