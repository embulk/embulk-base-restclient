package org.embulk.util.retryhelper.jetty93;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

import com.google.common.io.CharStreams;

import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.util.InputStreamResponseListener;

public class InputStreamJetty93ResponseEntityReader
        implements Jetty93ResponseReader<InputStream>
{
    public InputStreamJetty93ResponseEntityReader(long timeoutMillis)
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

    @Override
    public final String readResponseContentInString()
            throws Exception
    {
        final InputStream inputStream = this.readResponseContent();
        try (InputStreamReader inputStreamReader = new InputStreamReader(inputStream)) {
            return CharStreams.toString(inputStreamReader);
        }
    }

    private final InputStreamResponseListener listener;
    private final long timeoutMillis;
}
