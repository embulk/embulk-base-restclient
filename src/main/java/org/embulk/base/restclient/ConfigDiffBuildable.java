package org.embulk.base.restclient;

import org.embulk.config.ConfigDiff;

public interface ConfigDiffBuildable<T extends RestClientTaskBase>
{
    ConfigDiff buildConfigDiff(T task);
}
