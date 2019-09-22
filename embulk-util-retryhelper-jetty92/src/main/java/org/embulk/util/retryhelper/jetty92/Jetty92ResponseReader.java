package org.embulk.util.retryhelper.jetty92;

import org.eclipse.jetty.client.api.Response;

/**
 * Jetty92ResponseReader defines methods that read (understand) Jetty 9.3's response
 * through {@link org.eclipse.jetty.client.api.Response.Listener}s.
 *
 * Find some predefined {@code Jetty92ResponseReader}s such as {@link StringJetty92ResponseEntityReader}.
 */
public interface Jetty92ResponseReader<T>
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
