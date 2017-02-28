package org.embulk.util.retryhelper.jetty92;

import java.io.InputStream;
import java.util.concurrent.TimeUnit;

import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.util.InputStreamResponseListener;

public class InputStreamJetty92ResponseEntityReader
        implements Jetty92ResponseReader<InputStream>
{
    public InputStreamJetty92ResponseEntityReader(long timeoutMillis)
    {
        this.listener = new InputStreamResponseListener();
        this.timeoutMillis = timeoutMillis;
    }

    @Override
    public final Response.Listener getListener()
    {
        return this.listener;
    }

    @Override
    public final Response getResponse()
            throws Exception
    {
        return this.listener.get(this.timeoutMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public final InputStream readResponseContent()
            throws Exception
    {
        return this.listener.getInputStream();
    }

    private final InputStreamResponseListener listener;
    private final long timeoutMillis;
}
