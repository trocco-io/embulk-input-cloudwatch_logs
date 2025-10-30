package org.embulk.input.cloudwatch_logs;

import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.input.cloudwatch_logs.TestHelpers.CloudWatchLogsTestUtils;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.embulk.test.EmbulkTestRuntime;
import org.embulk.test.TestPageBuilderReader.MockPageOutput;
import org.embulk.test.TestingEmbulk;
import org.embulk.util.config.ConfigMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.embulk.input.cloudwatch_logs.CloudwatchLogsInputPlugin.CloudWatchLogsPluginTask;
import static org.embulk.input.cloudwatch_logs.TestHelpers.CONFIG_MAPPER_FACTORY;
import static org.junit.Assume.assumeNotNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TestCloudwatchLogsInputPlugin
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Rule
    public TestingEmbulk embulk = TestingEmbulk.builder()
            .registerPlugin(InputPlugin.class, "cloudwatch_logs", CloudwatchLogsInputPlugin.class)
            .build();

    private CloudwatchLogsInputPlugin plugin;

    private ConfigSource config;
    private final MockPageOutput output = new MockPageOutput();
    private PageBuilder pageBuilder;
    private CloudWatchLogsTestUtils testUtils;

    private static String embulkLogsTestRegion;
    private static String embulkLogsTestAccessKeyId;
    private static String embulkLogsTestSecretAccessKey;

    /*
     * This test case requires environment variables:
     *   EMBULK_LOGS_TEST_REGION
     *   EMBULK_LOGS_TEST_ACCESS_KEY_ID
     *   EMBULK_LOGS_TEST_SECRET_ACCESS_KEY
     * If the variables not set, the test case is skipped.
     */
    @BeforeClass
    public static void initializeConstantVariables()
    {
        embulkLogsTestRegion = System.getenv("EMBULK_LOGS_TEST_REGION");
        embulkLogsTestAccessKeyId = System.getenv("EMBULK_LOGS_TEST_ACCESS_KEY_ID");
        embulkLogsTestSecretAccessKey = System.getenv("EMBULK_LOGS_TEST_SECRET_ACCESS_KEY");
        assumeNotNull(embulkLogsTestRegion, embulkLogsTestAccessKeyId, embulkLogsTestSecretAccessKey);
    }

    @Before
    public void setUp()
    {
        String logGroupName = TestHelpers.getLogGroupName();
        String logStreamName = TestHelpers.getLogStreamName();

        if (plugin == null) {
            plugin = Mockito.spy(new CloudwatchLogsInputPlugin());
            config = runtime.getExec().newConfigSource()
                    .set("type", "cloudwatch_logs")
                    .set("log_group_name", logGroupName)
                    .set("log_stream_name", logStreamName)
                    .set("region", embulkLogsTestRegion)
                    .set("aws_access_key_id", embulkLogsTestAccessKeyId)
                    .set("aws_secret_access_key", embulkLogsTestSecretAccessKey);
            pageBuilder = Mockito.mock(PageBuilder.class);
        }
        doReturn(pageBuilder).when(plugin).getPageBuilder(Mockito.any(), Mockito.any());
        ConfigMapper configMapper = CONFIG_MAPPER_FACTORY.createConfigMapper();
        CloudWatchLogsPluginTask task = configMapper.map(config, CloudWatchLogsPluginTask.class);
        CloudwatchLogsInputPlugin plugin = new CloudwatchLogsInputPlugin();
        CloudWatchLogsClient logsClient = plugin.newLogsClient(task);
        testUtils = new CloudWatchLogsTestUtils(logsClient, logGroupName, logStreamName);
    }

    @After
    public void tearDown()
    {
        testUtils.clearLogGroup();
    }

    @Test
    public void test_simple()
    {
        testUtils.createLogStream();

        List<InputLogEvent> events = new ArrayList<>();
        Date d = new Date();
        for (int i = 0; i < 3; i++) {
            InputLogEvent event = InputLogEvent.builder()
                    .timestamp(d.getTime())
                    .message(String.format("CloudWatchLogs from Embulk take %d", i))
                    .build();
            events.add(event);
        }
        testUtils.putLogEvents(events);
        try {
            Thread.sleep(10000);
        }
        catch (InterruptedException e) {
            // NOP
        }

        plugin.transaction(config, new Control());
        verify(pageBuilder, times(3)).addRecord();
        verify(pageBuilder, times(1)).finish();
    }

    @Test
    public void configuredRegion()
    {
        ConfigMapper configMapper = CONFIG_MAPPER_FACTORY.createConfigMapper();
        CloudWatchLogsPluginTask task = configMapper.map(config.deepCopy()
                .set("region", "ap-southeast-2")
                .remove("endpoint"), CloudWatchLogsPluginTask.class);
        CloudwatchLogsInputPlugin plugin = new CloudwatchLogsInputPlugin();
        CloudWatchLogsClient logsClient = plugin.newLogsClient(task);

        // Should not be null
        assumeNotNull(logsClient);
    }

    private class Control implements InputPlugin.Control
    {
        @Override
        public List<TaskReport> run(final TaskSource taskSource, final Schema schema, final int taskCount)
        {
            List<TaskReport> reports = new ArrayList<>();
            for (int i = 0; i < taskCount; i++) {
                reports.add(plugin.run(taskSource, schema, i, output));
            }
            return reports;
        }
    }
}
