package com.google.migration.common;

import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import java.io.IOException;

/**
 * {@link SecretManagerUtils} class is a class that takes in a Secret Version of the form
 * projects/{project}/secrets/{secret}/versions/{secret_version} and returns the secret value in
 * Secret Manager.
 */
public class SecretManagerUtils {

  /**
   * Calls Secret Manager with a Secret Version and returns the secret value.
   *
   * @param secretVersion Secret Version of the form
   *     projects/{project}/secrets/{secret}/versions/{secret_version}
   * @return the secret value in Secret Manager
   */
  public static String getSecret(String secretVersion) {
    SecretVersionName secretVersionName = parseSecretVersion(secretVersion);

    try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
      AccessSecretVersionResponse response = client.accessSecretVersion(secretVersionName);
      return response.getPayload().getData().toStringUtf8();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Parses a Secret Version and returns a {@link SecretVersionName}.
   *
   * @param secretVersion Secret Version of the form
   *     projects/{project}/secrets/{secret}/versions/{secret_version}
   * @return {@link SecretVersionName}
   */
  private static SecretVersionName parseSecretVersion(String secretVersion) {
    if (SecretVersionName.isParsableFrom(secretVersion)) {
      return SecretVersionName.parse(secretVersion);
    } else {
      throw new IllegalArgumentException(
          "Provided Secret must be in the form"
              + " projects/{project}/secrets/{secret}/versions/{secret_version}");
    }
  }
}

