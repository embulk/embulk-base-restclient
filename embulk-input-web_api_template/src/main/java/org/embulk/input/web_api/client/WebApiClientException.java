package org.embulk.input.web_api.client;

public class WebApiClientException
        extends RuntimeException
{
    private final int code;

    public WebApiClientException(int code, String reason)
    {
        super(reason);
        this.code = code;
    }

    public int getCode()
    {
        return code;
    }
}
