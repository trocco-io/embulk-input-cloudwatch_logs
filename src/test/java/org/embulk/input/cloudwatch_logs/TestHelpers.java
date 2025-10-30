package org.embulk.input.cloudwatch_logs;

import org.embulk.util.config.ConfigMapperFactory;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogGroupRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogStreamRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DeleteLogGroupRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.ResourceNotFoundException;

import java.util.Date;
import java.util.List;
import java.util.UUID;

public final class TestHelpers
{
    public static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory.builder().addDefaultModules().build();

    private TestHelpers() {}

    public static String getLogGroupName()
    {
        Date d = new Date();
        return String.format("embulk-input-cloudwatch-test-%d", d.getTime());
    }

    public static String getLogStreamName()
    {
        UUID uuid = UUID.randomUUID();
        return uuid.toString();
    }

    static class CloudWatchLogsTestUtils
    {
        private final CloudWatchLogsClient logs;
        private final String logGroupName;
        private final String logStreamName;

        public CloudWatchLogsTestUtils(CloudWatchLogsClient logs, String logGroupName, String logStreamName)
        {
            this.logs = logs;
            this.logGroupName = logGroupName;
            this.logStreamName = logStreamName;
        }

        public void clearLogGroup()
        {
            DeleteLogGroupRequest request = DeleteLogGroupRequest.builder()
                    .logGroupName(logGroupName)
                    .build();
            try {
                logs.deleteLogGroup(request);
            }
            catch (ResourceNotFoundException ex) {
                // Just ignored.
            }
        }

        public void createLogStream()
        {
            CreateLogGroupRequest groupRequest = CreateLogGroupRequest.builder()
                    .logGroupName(logGroupName)
                    .build();
            logs.createLogGroup(groupRequest);

            CreateLogStreamRequest streamRequest = CreateLogStreamRequest.builder()
                    .logGroupName(logGroupName)
                    .logStreamName(logStreamName)
                    .build();
            logs.createLogStream(streamRequest);
        }

        public void putLogEvents(List<InputLogEvent> events)
        {
            PutLogEventsRequest request = PutLogEventsRequest.builder()
                    .logGroupName(logGroupName)
                    .logStreamName(logStreamName)
                    .logEvents(events)
                    .build();
            logs.putLogEvents(request);
        }
    }
}
