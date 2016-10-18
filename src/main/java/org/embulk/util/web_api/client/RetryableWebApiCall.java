package org.embulk.util.web_api.client;

import javax.ws.rs.core.Response;

public interface RetryableWebApiCall
{
    Response request();
    boolean isNotRetryable(Exception e);
}
