package org.embulk.base.restclient.client;

import javax.ws.rs.core.Response;

public interface RetryableWebApiCall
{
    Response request();
    boolean isNotRetryable(Exception e);
}
