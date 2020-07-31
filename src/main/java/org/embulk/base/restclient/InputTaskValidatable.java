package org.embulk.base.restclient;

public interface InputTaskValidatable<T extends RestClientInputTaskBase> {
    void validateInputTask(T task);
}
