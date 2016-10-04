package org.embulk.input.web_api.client;

public interface RetryableWebApiCall<PluginTask, Response>
{
    Response execute(PluginTask task);

    boolean isNotRetryableException(Exception e);
    boolean isNotRetryableResponse(Exception e);
}
