package org.embulk.input.web_api.client;

import javax.ws.rs.core.Response;

public interface RetryableWebApiCall<TASK, RESPONSE>
{
    Response request(TASK task);
    RESPONSE readResponse(Response response);
    boolean isNotRetryable(Exception e);
}
