package org.embulk.util.web_api.client;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

public abstract class AbstractRetryableWebApiCall<TASK, RESPONSE>
        implements RetryableWebApiCall<TASK, RESPONSE>
{
    @Override
    public abstract Response request(TASK task);

    @Override
    public abstract RESPONSE readResponse(Response response);

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