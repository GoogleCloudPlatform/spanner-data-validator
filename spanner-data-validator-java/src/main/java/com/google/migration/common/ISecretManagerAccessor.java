package com.google.migration.common;

/** Interface to access secret from Secret Manager. */
public interface ISecretManagerAccessor {

  String getSecret(String secretName);
}

