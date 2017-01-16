package org.embulk.base.restclient.request;

import org.embulk.base.restclient.ClientCreatable;
import org.embulk.base.restclient.RestClientTaskBase;

public class AutoCloseableClient<T extends RestClientTaskBase>
        implements AutoCloseable
{
    public AutoCloseableClient(T task, ClientCreatable<T> clientCreator)
    {
        this.client = clientCreator.createClient(task);
    }

    public javax.ws.rs.client.Client getClient()
    {
        return this.client;
    }

    @Override
    public void close()
    {
        if (this.client != null) {
            this.client.close();
        }
    }

    private final javax.ws.rs.client.Client client;
}
