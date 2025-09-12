package org.embulk.input.cloudwatch_logs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;

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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.AWSLogsClientBuilder;
import com.amazonaws.services.logs.model.DescribeLogStreamsRequest;
import com.amazonaws.services.logs.model.DescribeLogStreamsResult;
import com.amazonaws.services.logs.model.GetLogEventsRequest;
import com.amazonaws.services.logs.model.GetLogEventsResult;
import com.amazonaws.services.logs.model.LogStream;
import com.amazonaws.services.logs.model.OutputLogEvent;
import com.google.common.annotations.VisibleForTesting;

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

        AWSLogs client = newLogsClient(task);
        CloudWatchLogsDrainer drainer = new CloudWatchLogsDrainer(task, client);
        int totalRecords = 0;
        if (task.getUseLogStreamNamePrefix()) {
            List<LogStream> defaultLogStream = new ArrayList<>();
            List<LogStream> logStreams = drainer.describeLogStreams(defaultLogStream, null);
            try (final PageBuilder pageBuilder = getPageBuilder(schema, output)) {
                for (LogStream stream : logStreams) {
                    String logStreamName = stream.getLogStreamName();
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
            GetLogEventsResult result = drainer.getEvents(logStreamName, nextToken);
            List<OutputLogEvent> events = result.getEvents();
            for (OutputLogEvent event : events) {
                // TODO: Use Instant instead of Timestamp
                pageBuilder.setTimestamp(0, org.embulk.spi.time.Timestamp.ofEpochMilli(event.getTimestamp()));
                pageBuilder.setString(1, event.getMessage());

                pageBuilder.addRecord();
                recordCount++;
            }
            String nextForwardToken = result.getNextForwardToken();
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
     * @return AWSLogs
     */
    protected AWSLogs newLogsClient(PluginTask task)
    {
        return defaultLogsClientBuilder(task).build();
    }

    /**
     * A base builder for the subclasses to then customize.builder
     * @param task Embulk plugin
     * @return AWSLogsClientBuilder
     **/
    protected AWSLogsClientBuilder defaultLogsClientBuilder(PluginTask task)
    {
        return AWSLogsClientBuilder
                .standard()
                .withCredentials(getCredentialsProvider(task))
                .withClientConfiguration(getClientConfiguration(task));
    }

    protected AWSCredentialsProvider getCredentialsProvider(PluginTask task)
    {
        return AwsCredentials.getAWSCredentialsProvider(task);
    }

    protected ClientConfiguration getClientConfiguration(PluginTask task)
    {
        ClientConfiguration clientConfig = new ClientConfiguration();

        //clientConfig.setProtocol(Protocol.HTTP);
        clientConfig.setMaxConnections(50); // SDK default: 50
        clientConfig.setMaxErrorRetry(5); // Increased from default 3 to handle connection issues
        clientConfig.setSocketTimeout(10 * 60 * 1000); // Increased from 8 minutes to 10 minutes
        clientConfig.setConnectionTimeout(60 * 1000); // Set connection timeout to 60 seconds
        clientConfig.setRequestTimeout(15 * 60 * 1000); // Set request timeout to 15 minutes
        // Use default retry policy with exponential backoff instead of NO_RETRY_POLICY
        clientConfig.setRetryPolicy(PredefinedRetryPolicies.getDefaultRetryPolicy());
        clientConfig.setConnectionTTL(60 * 1000); // Increased from 30 seconds to 60 seconds
        // Enable TCP keep alive to maintain connections
        clientConfig.setUseTcpKeepAlive(true);
        // TODO: implement http proxy

        return clientConfig;
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
        private final AWSLogs client;
        private final PluginTask task;

        public CloudWatchLogsDrainer(PluginTask task, AWSLogs client)
        {
            this.client = client;
            this.task = task;
        }

        private GetLogEventsResult getEvents(String logStreamName, String nextToken)
        {
            try {
                String logGroupName = task.getLogGroupName();
                GetLogEventsRequest request = new GetLogEventsRequest()
                        .withLogGroupName(logGroupName)
                        .withLogStreamName(logStreamName)
                        .withStartFromHead(true);
                String timeRangeFormat = DEFAULT_DATE_FORMAT;
                if (task.getTimeRangeFormat().isPresent()) {
                    timeRangeFormat = task.getTimeRangeFormat().get();
                }
                if (task.getStartTime().isPresent()) {
                    String startTimeStr = task.getStartTime().get();
                    Date startTime = DateUtils.parseDateStr(startTimeStr, Collections.singletonList(timeRangeFormat));
                    request.setStartTime(startTime.getTime());
                }
                if (task.getEndTime().isPresent()) {
                    String endTimeStr = task.getEndTime().get();
                    Date endTime = DateUtils.parseDateStr(endTimeStr, Collections.singletonList(timeRangeFormat));
                    request.setEndTime(endTime.getTime());
                }
                if (nextToken != null) {
                    request.setNextToken(nextToken);
                }
                GetLogEventsResult response = client.getLogEvents(request);

                return response;
            }
            catch (AmazonServiceException ex) {
                if (ex.getErrorType().equals(AmazonServiceException.ErrorType.Client)) {
                    // HTTP 40x errors. auth error etc. See AWS document for the full list:
                    // https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/CommonErrors.html
                    if (ex.getStatusCode() != 400   // 404 Bad Request is unexpected error
                        || "ExpiredToken".equalsIgnoreCase(ex.getErrorCode())) { // if statusCode == 400 && errorCode == ExpiredToken => throws ConfigException
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
                DescribeLogStreamsRequest request = new DescribeLogStreamsRequest();
                request.setLogGroupName(logGroupName);
                if (nextToken != null) {
                    request.setNextToken(nextToken);
                }
                if (task.getLogStreamName().isPresent()) {
                    request.setLogStreamNamePrefix(task.getLogStreamName().get());
                }

                DescribeLogStreamsResult response = client.describeLogStreams(request);
                if (!logStreams.isEmpty()) {
                    logStreams.addAll(response.getLogStreams());
                }
                else {
                    logStreams = response.getLogStreams();
                }
                if (response.getNextToken() != null) {
                    logStreams = describeLogStreams(logStreams, response.getNextToken());
                }

                return logStreams;
            }
            catch (AmazonServiceException ex) {
                if (ex.getErrorType().equals(AmazonServiceException.ErrorType.Client)) {
                    // HTTP 40x errors. auth error etc. See AWS document for the full list:
                    // https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/CommonErrors.html
                    if (ex.getStatusCode() != 400   // 404 Bad Request is unexpected error
                        || "ExpiredToken".equalsIgnoreCase(ex.getErrorCode())) { // if statusCode == 400 && errorCode == ExpiredToken => throws ConfigException
                        throw new ConfigException(ex);
                    }
                }
                throw ex;
            }
        }
    }
}
