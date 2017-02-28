package org.embulk.util.retryhelper.jetty93;

import org.eclipse.jetty.client.api.Response;

/**
 * Jetty93ResponseReader defines methods that read (understand) Jetty 9.3's response through {@code Listener}s.
 *
 * Find some predefined {@code Jetty93ResponseReader}s such as {@code StringJetty93ResponseEntityReader}.
 */
public interface Jetty93ResponseReader<T>
{
    Response.Listener getListener();
    Response getResponse() throws Exception;
    T readResponseContent() throws Exception;
}
