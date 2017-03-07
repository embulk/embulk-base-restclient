package org.embulk.util.retryhelper.jetty92;

import org.eclipse.jetty.client.api.Response;

/**
 * Jetty92ResponseReader defines methods that read (understand) Jetty 9.3's response through {@code Listener}s.
 *
 * Find some predefined {@code Jetty92ResponseReader}s such as {@code StringJetty92ResponseEntityReader}.
 */
public interface Jetty92ResponseReader<T>
{
    Response.Listener getListener();
    Response getResponse() throws Exception;
    T readResponseContent() throws Exception;
    String readResponseContentInString() throws Exception;
}
