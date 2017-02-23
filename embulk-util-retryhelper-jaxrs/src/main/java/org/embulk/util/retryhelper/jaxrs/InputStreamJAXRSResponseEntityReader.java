package org.embulk.util.retryhelper.jaxrs;

import java.io.InputStream;

public class InputStreamJAXRSResponseEntityReader
        implements JAXRSResponseReader<InputStream>
{
    public final InputStream readResponse(javax.ws.rs.core.Response response)
    {
        return response.readEntity(InputStream.class);
    }
}
