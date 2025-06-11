package com.google.migration.common;

import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;
import org.apache.beam.sdk.io.jdbc.JdbcIO.DataSourceConfiguration;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class CustomDataSourceProviderFromDataSourceConfiguration
    implements SerializableFunction<Void, DataSource> {
  private static final ConcurrentHashMap<DataSourceConfiguration, DataSource> instances =
      new ConcurrentHashMap<>();
  public final DataSourceConfiguration config;

  public CustomDataSourceProviderFromDataSourceConfiguration(DataSourceConfiguration config) {
    this.config = config;
  }

  public static SerializableFunction<Void, DataSource> of(DataSourceConfiguration config) {
    return new com.google.migration.common.CustomDataSourceProviderFromDataSourceConfiguration(config);
  }

  @Override
  public DataSource apply(Void input) {
    return instances.computeIfAbsent(config, DataSourceConfiguration::buildDatasource);
  }
}