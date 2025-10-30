package org.embulk.input.cloudwatch_logs;

import com.google.common.annotations.VisibleForTesting;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.input.cloudwatch_logs.aws.AwsCredentials;
import org.embulk.input.cloudwatch_logs.aws.AwsCredentialsTask;
import org.embulk.input.cloudwatch_logs.utils.DateUtils;
import org.embulk.spi.Exec;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.util.config.TaskMapper;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.core.retry.backoff.EqualJitterBackoffStrategy;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClientBuilder;
import software.amazon.awssdk.services.cloudwatchlogs.model.CloudWatchLogsException;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetLogEventsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.LogStream;
import software.amazon.awssdk.services.cloudwatchlogs.model.OutputLogEvent;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;

public abstract class AbstractCloudwatchLogsInputPlugin
        implements InputPlugin
{
    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory.builder().addDefaultModules().build();
    private static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public interface PluginTask
            extends AwsCredentialsTask, Task
    {
        @Config("log_group_name")
        public String getLogGroupName();

        @Config("log_stream_name")
        @ConfigDefault("null")
        public Optional<String> getLogStreamName();

        @Config("use_log_stream_name_prefix")
        @ConfigDefault("false")
        public boolean getUseLogStreamNamePrefix();

        @Config("start_time")
        @ConfigDefault("null")
        public Optional<String> getStartTime();

        @Config("end_time")
        @ConfigDefault("null")
        public Optional<String> getEndTime();

        @Config("time_range_format")
        @ConfigDefault("null")
        public Optional<String> getTimeRangeFormat();

        @Config("column_name")
        @ConfigDefault("\"message\"")
        public String getColumnName();

        @Config("stop_when_file_not_found")
        @ConfigDefault("false")
        boolean getStopWhenFileNotFound();
    }

    protected abstract Class<? extends PluginTask> getTaskClass();

    @Override
    public ConfigDiff transaction(ConfigSource config,
            InputPlugin.Control control)
    {
        ConfigMapper configMapper = CONFIG_MAPPER_FACTORY.createConfigMapper();
        PluginTask task = configMapper.map(config, getTaskClass());

        Schema schema = new Schema.Builder()
                .add("timestamp", Types.TIMESTAMP)
                .add(task.getColumnName(), Types.STRING)
                .build();
        int taskCount = 1;  // number of run() method calls
        String timeRangeFormat = DEFAULT_DATE_FORMAT;
        if (task.getTimeRangeFormat().isPresent()) {
            timeRangeFormat = task.getTimeRangeFormat().get();
        }
        if (task.getStartTime().isPresent() && task.getEndTime().isPresent()) {
            Date startTime = DateUtils.parseDateStr(task.getStartTime().get(),
                                                    Collections.singletonList(timeRangeFormat));
            Date endTime = DateUtils.parseDateStr(task.getEndTime().get(),
                                                  Collections.singletonList(timeRangeFormat));
            if (endTime.before(startTime)) {
                throw new ConfigException(String.format("endTime(%s) must not be earlier than startTime(%s).",
                                                        task.getEndTime().get(),
                                                        task.getStartTime().get()));
            }
        }

        return resume(task.toTaskSource(), schema, taskCount, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int taskCount,
            InputPlugin.Control control)
    {
        control.run(taskSource, schema, taskCount);
        return CONFIG_MAPPER_FACTORY.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource,
            Schema schema, int taskCount,
            List<TaskReport> successTaskReports)
    {
        // Do nothing.
    }

    @Override
    public TaskReport run(TaskSource taskSource,
            Schema schema, int taskIndex,
            PageOutput output)
    {
        TaskMapper taskMapper = CONFIG_MAPPER_FACTORY.createTaskMapper();
        PluginTask task = taskMapper.map(taskSource, getTaskClass());

        CloudWatchLogsClient client = newLogsClient(task);
        CloudWatchLogsDrainer drainer = new CloudWatchLogsDrainer(task, client);
        int totalRecords = 0;
        if (task.getUseLogStreamNamePrefix()) {
            List<LogStream> defaultLogStream = new ArrayList<>();
            List<LogStream> logStreams = drainer.describeLogStreams(defaultLogStream, null);
            try (final PageBuilder pageBuilder = getPageBuilder(schema, output)) {
                for (LogStream stream : logStreams) {
                    String logStreamName = stream.logStreamName();
                    totalRecords += addRecords(drainer, pageBuilder, logStreamName);
                }

                pageBuilder.finish();
            }
        }
        else {
            try (final PageBuilder pageBuilder = getPageBuilder(schema, output)) {
                String logStreamName = null;
                if (task.getLogStreamName().isPresent()) {
                    logStreamName = task.getLogStreamName().get();
                }
                totalRecords += addRecords(drainer, pageBuilder, logStreamName);

                pageBuilder.finish();
            }
        }

        if (totalRecords == 0 && task.getStopWhenFileNotFound()) {
            throw new ConfigException("No log record is found. \"stop_when_file_not_found\" option is \"true\".");
        }

        return CONFIG_MAPPER_FACTORY.newTaskReport();
    }

    @SuppressWarnings("deprecation") // TODO: For compatibility with Embulk v0.9
    private int addRecords(CloudWatchLogsDrainer drainer, PageBuilder pageBuilder, String logStreamName)
    {
        String nextToken = null;
        int recordCount = 0;
        while (true) {
            GetLogEventsResponse result = drainer.getEvents(logStreamName, nextToken);
            List<OutputLogEvent> events = result.events();
            for (OutputLogEvent event : events) {
                // TODO: Use Instant instead of Timestamp
                pageBuilder.setTimestamp(0, org.embulk.spi.time.Timestamp.ofEpochMilli(event.timestamp()));
                pageBuilder.setString(1, event.message());

                pageBuilder.addRecord();
                recordCount++;
            }
            String nextForwardToken = result.nextForwardToken();
            if (nextForwardToken == null || nextForwardToken.equals(nextToken)) {
                break;
            }
            nextToken = nextForwardToken;
        }

        return recordCount;
    }

    /**
     * Provide an overridable default client.
     * Since this returns an immutable object, it is not for any further customizations by mutating,
     * Subclass's customization should be done through {@link AbstractCloudwatchLogsInputPlugin#defaultLogsClientBuilder}.
     * @param task Embulk plugin task
     * @return CloudWatchLogsClient
     */
    protected CloudWatchLogsClient newLogsClient(PluginTask task)
    {
        return defaultLogsClientBuilder(task).build();
    }

    /**
     * A base builder for the subclasses to then customize.builder
     * @param task Embulk plugin
     * @return CloudWatchLogsClientBuilder
     **/
    protected CloudWatchLogsClientBuilder defaultLogsClientBuilder(PluginTask task)
    {
        return CloudWatchLogsClient.builder()
                .credentialsProvider(getCredentialsProvider(task))
                .httpClientBuilder(getHttpClientBuilder())
                .overrideConfiguration(getClientOverrideConfiguration());
    }

    protected AwsCredentialsProvider getCredentialsProvider(PluginTask task)
    {
        return AwsCredentials.getAWSCredentialsProvider(task);
    }

    protected ApacheHttpClient.Builder getHttpClientBuilder()
    {
        return ApacheHttpClient.builder()
                .maxConnections(50)
                .connectionTimeout(Duration.ofSeconds(60))
                .socketTimeout(Duration.ofMinutes(10))
                .connectionTimeToLive(Duration.ofSeconds(60))
                .tcpKeepAlive(true);
    }

    protected ClientOverrideConfiguration getClientOverrideConfiguration()
    {
        BackoffStrategy backoffStrategy = EqualJitterBackoffStrategy.builder()
                .baseDelay(Duration.ofMillis(100))
                .maxBackoffTime(Duration.ofSeconds(20))
                .build();

        RetryPolicy retryPolicy = RetryPolicy.builder()
                .numRetries(5)
                .backoffStrategy(backoffStrategy)
                .retryCondition(RetryCondition.defaultRetryCondition())
                .build();

        return ClientOverrideConfiguration.builder()
                .apiCallTimeout(Duration.ofMinutes(15))
                .apiCallAttemptTimeout(Duration.ofMinutes(10))
                .retryPolicy(retryPolicy)
                .build();
    }

    @Override
    public ConfigDiff guess(ConfigSource config)
    {
        return CONFIG_MAPPER_FACTORY.newConfigDiff();
    }

    @VisibleForTesting
    @SuppressWarnings("deprecation") // TODO: For compatibility with Embulk v0.9
    public PageBuilder getPageBuilder(final Schema schema, final PageOutput output)
    {
        return new PageBuilder(Exec.getBufferAllocator(), schema, output); // TODO: Use Exec#getPageBuilder
    }

    @VisibleForTesting
    static class CloudWatchLogsDrainer
    {
        private final CloudWatchLogsClient client;
        private final PluginTask task;

        public CloudWatchLogsDrainer(PluginTask task, CloudWatchLogsClient client)
        {
            this.client = client;
            this.task = task;
        }

        private GetLogEventsResponse getEvents(String logStreamName, String nextToken)
        {
            try {
                String logGroupName = task.getLogGroupName();
                GetLogEventsRequest.Builder requestBuilder = GetLogEventsRequest.builder()
                        .logGroupName(logGroupName)
                        .logStreamName(logStreamName)
                        .startFromHead(true);

                String timeRangeFormat = DEFAULT_DATE_FORMAT;
                if (task.getTimeRangeFormat().isPresent()) {
                    timeRangeFormat = task.getTimeRangeFormat().get();
                }
                if (task.getStartTime().isPresent()) {
                    String startTimeStr = task.getStartTime().get();
                    Date startTime = DateUtils.parseDateStr(startTimeStr, Collections.singletonList(timeRangeFormat));
                    requestBuilder.startTime(startTime.getTime());
                }
                if (task.getEndTime().isPresent()) {
                    String endTimeStr = task.getEndTime().get();
                    Date endTime = DateUtils.parseDateStr(endTimeStr, Collections.singletonList(timeRangeFormat));
                    requestBuilder.endTime(endTime.getTime());
                }
                if (nextToken != null) {
                    requestBuilder.nextToken(nextToken);
                }

                GetLogEventsResponse response = client.getLogEvents(requestBuilder.build());

                return response;
            }
            catch (CloudWatchLogsException ex) {
                if (ex.isThrottlingException() || ex.statusCode() >= 500) {
                    // Server errors or throttling - let retry policy handle it
                    throw ex;
                }
                if (ex.statusCode() >= 400 && ex.statusCode() < 500) {
                    // Client errors
                    if (ex.statusCode() != 400 || "ExpiredToken".equalsIgnoreCase(ex.awsErrorDetails().errorCode())) {
                        throw new ConfigException(ex);
                    }
                }
                throw ex;
            }
        }

        private List<LogStream> describeLogStreams(List<LogStream> logStreams, String nextToken)
        {
            try {
                String logGroupName = task.getLogGroupName();
                DescribeLogStreamsRequest.Builder requestBuilder = DescribeLogStreamsRequest.builder()
                        .logGroupName(logGroupName);

                if (nextToken != null) {
                    requestBuilder.nextToken(nextToken);
                }
                if (task.getLogStreamName().isPresent()) {
                    requestBuilder.logStreamNamePrefix(task.getLogStreamName().get());
                }

                DescribeLogStreamsResponse response = client.describeLogStreams(requestBuilder.build());
                if (!logStreams.isEmpty()) {
                    logStreams.addAll(response.logStreams());
                }
                else {
                    logStreams = new ArrayList<>(response.logStreams());
                }
                if (response.nextToken() != null) {
                    logStreams = describeLogStreams(logStreams, response.nextToken());
                }

                return logStreams;
            }
            catch (CloudWatchLogsException ex) {
                if (ex.isThrottlingException() || ex.statusCode() >= 500) {
                    // Server errors or throttling - let retry policy handle it
                    throw ex;
                }
                if (ex.statusCode() >= 400 && ex.statusCode() < 500) {
                    // Client errors
                    if (ex.statusCode() != 400 || "ExpiredToken".equalsIgnoreCase(ex.awsErrorDetails().errorCode())) {
                        throw new ConfigException(ex);
                    }
                }
                throw ex;
            }
        }
    }
}
