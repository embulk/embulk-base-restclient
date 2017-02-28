package org.embulk.util.retryhelper.jaxrs;

public interface JAXRSClientCreator
{
    public javax.ws.rs.client.Client create();
}
