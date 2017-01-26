package org.embulk.base.restclient.request;

import java.io.InputStream;

// TODO(dmikurube): Have some implemented interfaces of ResponseReadable with Java 8 default methods.
// https://docs.oracle.com/javase/tutorial/java/IandI/defaultmethods.html
public class ResponseReaders {
    public static javax.ws.rs.core.Response readResponseAsIs(javax.ws.rs.core.Response response)
    {
        return response;
    }

    public static String readResponseAsString(javax.ws.rs.core.Response response)
    {
        // |javax.ws.rs.ProcessingException| can happen by read time-out.
        return response.readEntity(String.class);
    }

    public static InputStream readResponseAsInputStream(javax.ws.rs.core.Response response)
    {
        // |javax.ws.rs.ProcessingException| can happen by read time-out.
        return response.readEntity(InputStream.class);
    }
}
