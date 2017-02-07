package org.embulk.base.restclient;

public interface ResumeConfigurable<T extends RestClientTaskBase>
{
    public boolean isResuming(T task);
}
