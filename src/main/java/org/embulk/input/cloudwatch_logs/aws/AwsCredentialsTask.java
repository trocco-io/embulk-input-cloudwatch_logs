package org.embulk.input.cloudwatch_logs.aws;

import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.units.LocalFile;

import java.util.Optional;

public interface AwsCredentialsTask
    extends AwsCredentialsConfig
{
    @Override
    @Config("authentication_method")
    @ConfigDefault("\"basic\"")
    String getAuthenticationMethod();

    @Override
    @Config("aws_access_key_id")
    @ConfigDefault("null")
    Optional<String> getAwsAccessKeyId();

    @Override
    @Config("aws_secret_access_key")
    @ConfigDefault("null")
    Optional<String> getAwsSecretAccessKey();

    @Override
    @Config("aws_session_token")
    @ConfigDefault("null")
    Optional<String> getAwsSessionToken();

    @Override
    @Config("aws_profile_file")
    @ConfigDefault("null")
    Optional<LocalFile> getAwsProfileFile();

    @Override
    @Config("aws_profile_name")
    @ConfigDefault("null")
    Optional<String> getAwsProfileName();
}
