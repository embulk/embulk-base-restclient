package org.embulk.base.restclient;

import java.util.List;

import org.embulk.config.ConfigDiff;
import org.embulk.config.TaskReport;
import org.embulk.spi.Schema;

public interface ConfigDiffBuildable<T extends RestClientTaskBase>
{
    ConfigDiff buildConfigDiff(T task, Schema schema, int taskCount, List<TaskReport> taskReports);
}
