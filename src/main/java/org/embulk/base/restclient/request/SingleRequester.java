package org.embulk.base.restclient.request;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

public abstract class SingleRequester
{
    public abstract Response request();

    public boolean isNotRetryable(Exception e)
    {
        if (e instanceof WebApplicationException) {
            return ((WebApplicationException) e).getResponse().getStatus() / 100 == 4;
        }
        else {
            return false;
        }
    }
}
