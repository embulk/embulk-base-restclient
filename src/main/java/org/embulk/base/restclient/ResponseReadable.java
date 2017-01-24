package org.embulk.base.restclient;

/**
 * ResponseReadable defines a method that reads (understands) JAX-RS |Response| to another type.
 *
 * This is prepared so that reading a JAX-RS |Response| can be retried
 * in |RetryHelper|. If |RetryHelper| returns just JAX-RS |Response|,
 * developers need to call |Response#readEntity| or else by themselves,
 * and retry by themselves as well.
 *
 * TODO(dmikurube): Have some implemented interfaces of ResponseReadable with Java 8 default methods.
 * https://docs.oracle.com/javase/tutorial/java/IandI/defaultmethods.html
 */
public interface ResponseReadable<T>
{
    T readResponse(javax.ws.rs.core.Response response);
}
