package org.embulk.base.restclient;

public interface InputTaskValidatable<T extends RestClientInputTaskBase>
{
    public void validateInputTask(T task);
}
