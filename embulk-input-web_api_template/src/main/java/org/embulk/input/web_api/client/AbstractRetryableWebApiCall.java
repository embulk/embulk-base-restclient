package org.embulk.input.web_api.client;

import javax.ws.rs.core.Response;

public abstract class AbstractRetryableWebApiCall<TASK, RESPONSE>
        implements RetryableWebApiCall<TASK, RESPONSE>
{
    @Override
    public abstract Response request(TASK task);

    @Override
    public abstract RESPONSE readResponse(Response response);

    @Override
    public boolean isNotRetryableException(Exception e)
    {
        return false;
    }

    @Override
    public boolean isNotRetryableResponse(Exception e)
    {
        return false;
    }
}
