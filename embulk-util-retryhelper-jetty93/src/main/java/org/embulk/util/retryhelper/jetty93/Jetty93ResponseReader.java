package org.embulk.util.retryhelper.jetty93;

import org.eclipse.jetty.client.api.Response;

/**
 * Jetty93ResponseReader defines methods that read (understand) Jetty 9.3's response through {@code Listener}s.
 *
 * Find some predefined {@code Jetty93ResponseReader}s such as {@code StringJetty93ResponseEntityReader}.
 */
public interface Jetty93ResponseReader<T>
{
    /**
     * @return {@link Response.Listener}
     * @deprecated Use {@link #newListener()} instead.
     */
    @Deprecated
    Response.Listener getListener();

    /**
     * Obtains a new listener per request.
     * Make sure it is called before reading the response. I.e., it must be called on per-request basis.
     * @return {@link Response.Listener}
     */
    Response.Listener newListener();

    /**
     * @return Response as headers not waiting for contents.
     * @throws Exception
     */
    Response getResponse() throws Exception;

    T readResponseContent() throws Exception;

    String readResponseContentInString() throws Exception;
}
