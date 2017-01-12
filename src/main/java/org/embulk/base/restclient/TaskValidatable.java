package org.embulk.base.restclient;

public interface TaskValidatable<T extends RestClientTaskBase>
{
    public void validateTask(T task);
}
