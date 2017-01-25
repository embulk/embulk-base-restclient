package org.embulk.base.restclient.request;

public class StringResponseEntityReader
        implements ResponseReadable<String>
{
    public final String readResponse(javax.ws.rs.core.Response response)
    {
        return response.readEntity(String.class);
    }
}
