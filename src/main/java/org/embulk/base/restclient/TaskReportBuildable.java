package org.embulk.base.restclient;

import org.embulk.config.TaskReport;

public interface TaskReportBuildable<T extends RestClientTaskBase>
{
    TaskReport buildTaskReport(T task);
}
