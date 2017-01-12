package org.embulk.base.restclient;

public interface ClientCreatable<T extends RestClientTaskBase>
{
    public javax.ws.rs.client.Client createClient(T task);
}
