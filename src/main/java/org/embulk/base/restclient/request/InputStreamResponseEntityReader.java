package org.embulk.base.restclient.request;

import java.io.InputStream;

public class InputStreamResponseEntityReader
        implements ResponseReadable<InputStream>
{
    public final InputStream readResponse(javax.ws.rs.core.Response response)
    {
        return response.readEntity(InputStream.class);
    }
}
