package org.embulk.input.cloudwatch_logs;

import org.embulk.config.ConfigException;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;

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
    protected CloudWatchLogsClient newLogsClient(PluginTask task)
    {
        CloudWatchLogsPluginTask t = (CloudWatchLogsPluginTask) task;
        Optional<String> region = t.getRegion();

        if (region.isPresent()) {
            return super.defaultLogsClientBuilder(t)
                    .region(Region.of(region.get()))
                    .build();
        }
        else {
            throw new ConfigException("region is required");
        }
    }
}
