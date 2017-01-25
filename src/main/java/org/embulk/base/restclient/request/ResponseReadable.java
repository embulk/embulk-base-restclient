package org.embulk.base.restclient.request;

/**
 * ResponseReadable defines a method that reads (understands) JAX-RS {@code Response} to another type.
 *
 * This is prepared so that reading a JAX-RS {@code Response} can be retried in
 * {@code RetryHelper}. If {@code RetryHelper} returns just JAX-RS
 * {@code Response}, developers need to call {@code Response#readEntity} or else
 * by themselves, and retry by themselves as well.
 *
 * Find some predefined {@code ResponseReadable} implementations such as
 * {@code StringResponseEntityReader}.
 */
public interface ResponseReadable<T>
{
    T readResponse(javax.ws.rs.core.Response response);
}
