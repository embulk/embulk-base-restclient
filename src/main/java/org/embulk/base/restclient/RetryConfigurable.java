package org.embulk.base.restclient;

public interface RetryConfigurable<T extends RestClientTaskBase>
{
    public int configureMaximumRetries(T task);
    public int configureInitialRetryIntervalMillis(T task);
    public int configureMaximumRetryIntervalMillis(T task);
}
