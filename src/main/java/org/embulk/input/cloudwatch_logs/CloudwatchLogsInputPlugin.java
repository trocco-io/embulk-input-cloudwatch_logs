package org.embulk.input.cloudwatch_logs;

import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.AWSLogsClientBuilder;
import org.embulk.config.ConfigException;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;

import java.util.Optional;

public class CloudwatchLogsInputPlugin
        extends AbstractCloudwatchLogsInputPlugin
{
    public interface CloudWatchLogsPluginTask
            extends PluginTask
    {
        @Config("region")
        @ConfigDefault("null")
        Optional<String> getRegion();
    }

    @Override
    protected Class<? extends PluginTask> getTaskClass()
    {
        return CloudWatchLogsPluginTask.class;
    }

    @Override
    protected AWSLogs newLogsClient(PluginTask task)
    {
        CloudWatchLogsPluginTask t = (CloudWatchLogsPluginTask) task;
        Optional<String> region = t.getRegion();
        AWSLogsClientBuilder builder = super.defaultLogsClientBuilder(t);

        if (region.isPresent()) {
            builder.setRegion(region.get());
        }
        else {
            throw new ConfigException("region is required");
        }

        return builder.build();
    }
}
