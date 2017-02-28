package org.embulk.util.retryhelper.jetty92;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.util.ssl.SslContextFactory;

public final class DefaultJetty92ClientCreator
        implements Jetty92ClientCreator
{
    public DefaultJetty92ClientCreator(int connectTimeout,
                                       int idleTimeout)
    {
        this(new SslContextFactory(), connectTimeout, idleTimeout, true);
    }

    public DefaultJetty92ClientCreator(int connectTimeout,
                                       int idleTimeout,
                                       boolean tcpNoDelay)
    {
        this(new SslContextFactory(), connectTimeout, idleTimeout, tcpNoDelay);
    }

    public DefaultJetty92ClientCreator(SslContextFactory sslContextFactory,
                                       int connectTimeout,
                                       int idleTimeout)
    {
        this(sslContextFactory, connectTimeout, idleTimeout, true);
    }

    public DefaultJetty92ClientCreator(SslContextFactory sslContextFactory,
                                       int connectTimeout,
                                       int idleTimeout,
                                       boolean tcpNoDelay)
    {
        this.sslContextFactory = sslContextFactory;
        this.connectTimeout = connectTimeout;
        this.idleTimeout = idleTimeout;
        this.tcpNoDelay = tcpNoDelay;
    }

    @Override
    public HttpClient createAndStart()
            throws Exception
    {
        HttpClient client = new HttpClient(this.sslContextFactory);
        client.setConnectTimeout(this.connectTimeout);
        client.setIdleTimeout(this.idleTimeout);
        client.setTCPNoDelay(this.tcpNoDelay);
        client.start();
        return client;
    }

    private final SslContextFactory sslContextFactory;
    private final int connectTimeout;
    private final int idleTimeout;
    private final boolean tcpNoDelay;
}
