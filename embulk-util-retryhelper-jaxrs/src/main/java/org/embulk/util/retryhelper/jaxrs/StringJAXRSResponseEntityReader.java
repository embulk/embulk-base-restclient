package org.embulk.util.retryhelper.jaxrs;

public class StringJAXRSResponseEntityReader
        implements JAXRSResponseReader<String>
{
    public final String readResponse(javax.ws.rs.core.Response response)
    {
        return response.readEntity(String.class);
    }
}
