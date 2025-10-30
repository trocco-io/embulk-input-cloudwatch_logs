package org.embulk.input.cloudwatch_logs.aws;

import org.embulk.config.ConfigException;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.profiles.ProfileFile;

import java.nio.file.Paths;
import java.util.Optional;

public abstract class AwsCredentials
{
    private AwsCredentials()
    {
    }

    public static AwsCredentialsProvider getAWSCredentialsProvider(AwsCredentialsConfig task)
    {
        String awsSessionTokenOption = "aws_session_token";
        String awsAccessKeyIdOption = "aws_access_key_id";
        String awsSecretAccessKeyOption = "aws_secret_access_key";
        String awsProfileFileOption = "aws_profile_file";
        String awsProfileNameOption = "aws_profile_name";

        switch (task.getAuthenticationMethod()) {
        case "basic": {
            String accessKeyId = required(task.getAwsAccessKeyId(), "'" + awsAccessKeyIdOption + "', '" + awsSecretAccessKeyOption + "'");
            String secretAccessKey = required(task.getAwsSecretAccessKey(), "'" + awsSecretAccessKeyOption + "'");
            invalid(task.getAwsProfileFile(), awsProfileFileOption);
            invalid(task.getAwsProfileName(), awsProfileNameOption);

            return StaticCredentialsProvider.create(
                AwsBasicCredentials.create(accessKeyId, secretAccessKey)
            );
        }

        case "env":
            invalid(task.getAwsAccessKeyId(), awsAccessKeyIdOption);
            invalid(task.getAwsSecretAccessKey(), awsSecretAccessKeyOption);
            invalid(task.getAwsProfileFile(), awsProfileFileOption);
            invalid(task.getAwsProfileName(), awsProfileNameOption);

            return overwriteAwsCredentials(task, EnvironmentVariableCredentialsProvider.create());

        case "instance":
            invalid(task.getAwsAccessKeyId(), awsAccessKeyIdOption);
            invalid(task.getAwsSecretAccessKey(), awsSecretAccessKeyOption);
            invalid(task.getAwsProfileFile(), awsProfileFileOption);
            invalid(task.getAwsProfileName(), awsProfileNameOption);

            return InstanceProfileCredentialsProvider.create();

        case "profile":
            {
                invalid(task.getAwsAccessKeyId(), awsAccessKeyIdOption);
                invalid(task.getAwsSecretAccessKey(), awsSecretAccessKeyOption);

                String profileName = task.getAwsProfileName().orElse("default");
                ProfileCredentialsProvider.Builder builder = ProfileCredentialsProvider.builder()
                    .profileName(profileName);

                if (task.getAwsProfileFile().isPresent()) {
                    ProfileFile profileFile = ProfileFile.builder()
                        .content(Paths.get(task.getAwsProfileFile().get().getPath().toString()))
                        .type(ProfileFile.Type.CREDENTIALS)
                        .build();
                    builder.profileFile(profileFile);
                }

                return overwriteAwsCredentials(task, builder.build());
            }

        case "properties":
            invalid(task.getAwsAccessKeyId(), awsAccessKeyIdOption);
            invalid(task.getAwsSecretAccessKey(), awsSecretAccessKeyOption);
            invalid(task.getAwsProfileFile(), awsProfileFileOption);
            invalid(task.getAwsProfileName(), awsProfileNameOption);

            return overwriteAwsCredentials(task, SystemPropertyCredentialsProvider.create());

        case "anonymous":
            invalid(task.getAwsAccessKeyId(), awsAccessKeyIdOption);
            invalid(task.getAwsSecretAccessKey(), awsSecretAccessKeyOption);
            invalid(task.getAwsProfileFile(), awsProfileFileOption);
            invalid(task.getAwsProfileName(), awsProfileNameOption);

            return AnonymousCredentialsProvider.create();

        case "session":
            {
                String accessKeyId = required(task.getAwsAccessKeyId(),
                                              "'" + awsAccessKeyIdOption + "', '" + awsSecretAccessKeyOption + "', '" + awsSessionTokenOption + "'");
                String secretAccessKey = required(task.getAwsSecretAccessKey(), "'" + awsSecretAccessKeyOption + "', '" + awsSessionTokenOption + "'");
                String sessionToken = required(task.getAwsSessionToken(),
                                               "'" + awsSessionTokenOption + "'");
                invalid(task.getAwsProfileFile(), awsProfileFileOption);
                invalid(task.getAwsProfileName(), awsProfileNameOption);

                return StaticCredentialsProvider.create(
                    AwsSessionCredentials.create(accessKeyId, secretAccessKey, sessionToken)
                );
            }

        case "default":
            {
                invalid(task.getAwsAccessKeyId(), awsAccessKeyIdOption);
                invalid(task.getAwsSecretAccessKey(), awsSecretAccessKeyOption);
                invalid(task.getAwsProfileFile(), awsProfileFileOption);
                invalid(task.getAwsProfileName(), awsProfileNameOption);

                return DefaultCredentialsProvider.create();
            }

        default:
            throw new ConfigException(String.format("Unknown authentication_method '%s'. Supported methods are basic, env, instance, profile, properties, anonymous, and default.",
                        task.getAuthenticationMethod()));
        }
    }

    private static AwsCredentialsProvider overwriteAwsCredentials(AwsCredentialsConfig task, final AwsCredentialsProvider provider)
    {
        // If session token is provided, wrap with session credentials
        if (task.getAwsSessionToken().isPresent()) {
            software.amazon.awssdk.auth.credentials.AwsCredentials creds = provider.resolveCredentials();
            return StaticCredentialsProvider.create(
                AwsSessionCredentials.create(
                    creds.accessKeyId(),
                    creds.secretAccessKey(),
                    task.getAwsSessionToken().get()
                )
            );
        }
        return provider;
    }

    private static <T> T required(Optional<T> value, String message)
    {
        if (value.isPresent()) {
            return value.get();
        }
        else {
            throw new ConfigException("Required option is not set: " + message);
        }
    }

    private static <T> void invalid(Optional<T> value, String message)
    {
        if (value.isPresent()) {
            throw new ConfigException("Invalid option is set: " + message);
        }
    }
}
