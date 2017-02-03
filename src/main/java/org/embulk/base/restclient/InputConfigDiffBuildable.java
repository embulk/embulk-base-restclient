package org.embulk.base.restclient;

import org.embulk.config.ConfigDiff;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.Schema;

public interface InputConfigDiffBuildable<T extends RestClientInputTaskBase>
{
    ConfigDiff buildInputConfigDiff(T task, Schema schema, int taskCount, InputPlugin.Control control);
}
