package com.google.migration.common;

/** Implementation to access secret from Secret Manager. */
public class SecretManagerAccessorImpl implements ISecretManagerAccessor {

  public String getSecret(String secretName) {
    return SecretManagerUtils.getSecret(secretName);
  }
}
