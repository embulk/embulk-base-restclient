package org.embulk.util.web_api.client;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

public abstract class AbstractRetryableWebApiCall
        implements RetryableWebApiCall
{
    @Override
    public abstract Response request();

    @Override
    public boolean isNotRetryable(Exception e)
    {
        if (e instanceof WebApplicationException) {
            return ((WebApplicationException)e).getResponse().getStatus() / 100 == 4;
        }
        else {
            return false;
        }
    }
}