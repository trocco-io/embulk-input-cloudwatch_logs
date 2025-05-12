# Cloudwatch Logs input plugin for Embulk

## Overview

* **Plugin type**: input
* **Resume supported**: no
* **Cleanup supported**: yes
* **Guess supported**: no

## Configuration

- **log_group_name**: CloudWathcLogs log group name (string, required)

- **log_stream_name**: CloudWathcLogs log stream name (string, default: `null`)

- **use_log_stream_name_prefix**: Whether using **log_stream_name** as stream name prefix (boolean, default: `false`)

- **column_name**: Column name for CloudWatchLogs' message column (string, default: `"message"`)

- **start_time**: CloudWatchLogs start-time (string, optional, default: `null`)

- **end_time**: CloudWatchLogs end-time (string, optional, default: `null`)

- **time_range_format**: Time range format. Internally, it will be used as `yyyy-MM-dd HH:mm:ss` by default. (string, optional, default: `null`)

- **region** CloudWatchLogs region. Currently this should be required. (string, optional)

- **authentication_method**: name of mechanism to authenticate requests (basic, env, instance, profile, properties, anonymous, or session. default: basic)

  - "basic": uses access_key_id and secret_access_key to authenticate.

    - **aws_access_key_id**: AWS access key ID (string, required)

    - **aws_secret_access_key**: AWS secret access key (string, required)

  - "env": uses `AWS_ACCESS_KEY_ID` (or `AWS_ACCESS_KEY`) and `AWS_SECRET_KEY` (or `AWS_SECRET_ACCESS_KEY`) environment variables.

  - "instance": uses EC2 instance profile.

  - "profile": uses credentials written in a file. Format of the file is as following, where `[...]` is a name of profile.

    - **aws_profile_file**: path to a profiles file. (string, default: given by `AWS_CREDENTIAL_PROFILES_FILE` environment varialbe, or ~/.aws/credentials).

    - **aws_profile_name**: name of a profile. (string, default: `"default"`)

    ```
    [default]
    aws_access_key_id=YOUR_ACCESS_KEY_ID
    aws_secret_access_key=YOUR_SECRET_ACCESS_KEY

    [profile2]
    ...
    ```

  - "properties": uses aws.accessKeyId and aws.secretKey Java system properties.

  - "anonymous": uses anonymous access. This auth method can access only public files.

  - "session": uses temporary-generated access_key_id, secret_access_key and session_token.

    - **aws_access_key_id**: AWS access key ID (string, required)

    - **aws_secret_access_key**: AWS secret access key (string, required)

    - **aws_session_token**: session token (string, required)

  - "default": uses AWS SDK's default strategy to look up available credentials from runtime environment. This method behaves like the combination of the following methods.

    1. "env"
    1. "properties"
    1. "profile"
    1. "instance"

- **stop_when_file_not_found**: if true, check existence of files (boolean, default false)

## Example

```yaml
in:
  type: cloudwatch_logs
  log_group_name: fluentd
  log_stream_name: cloudwatchlogs_test
  use_log_stream_name_prefix: true
  region: ap-northeast-1
  aws_access_key_id: ABCXYZ123ABCXYZ123
  aws_secret_access_key: AbCxYz123aBcXyZ123
```

Specifying time range is also supported.
`start_time` and `end_time` are corresponds to `startTime` and `endTime` in GetLogEvents request.
They are handled as number of milliseconds after Jan 1, 1970 00:00:00 UTC.

ref: https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_GetLogEvents.html#API_GetLogEvents_RequestSyntax

```yaml
  start_time: 2020-01-18 00:00:00Z
  end_time: 2020-03-20 00:00:00Z
  time_range_format: "YY-MM-dd HH:mm:ss'Z'"
```

To use `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables:

```yaml
in:
  type: cloudwatch_logs
  log_group_name: fluentd
  log_stream_name: cloudwatchlogs_test
  use_log_stream_name_prefix: true
  region: ap-northeast-1
  authentication_method: env
```

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
