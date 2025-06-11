package com.google.migration.common;

import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.DataSourceConfiguration;
import org.apache.beam.sdk.io.jdbc.JdbcIO.DataSourceProviderFromDataSourceConfiguration;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.commons.dbcp2.DataSourceConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class CustomPoolableDataSourceProvider
    implements SerializableFunction<Void, DataSource> {
  private static final ConcurrentHashMap<DataSourceConfiguration, DataSource> instances =
      new ConcurrentHashMap<>();
  private final CustomDataSourceProviderFromDataSourceConfiguration config;

  private CustomPoolableDataSourceProvider(DataSourceConfiguration config) {
    this.config = new CustomDataSourceProviderFromDataSourceConfiguration(config);
  }

  public static SerializableFunction<Void, DataSource> of(DataSourceConfiguration config) {
    return new CustomPoolableDataSourceProvider(config);
  }

  @Override
  public DataSource apply(Void input) {
    return instances.computeIfAbsent(
        config.config,
        ignored -> {
          DataSource basicSource = config.apply(input);
          DataSourceConnectionFactory connectionFactory =
              new DataSourceConnectionFactory(basicSource);
          @SuppressWarnings("nullness") // apache.commons.dbcp2 not annotated
          PoolableConnectionFactory poolableConnectionFactory =
              new PoolableConnectionFactory(connectionFactory, null);
          GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
          poolConfig.setMinIdle(0);
          poolConfig.setMaxIdle(0);
          poolConfig.setMinEvictableIdleTimeMillis(10000);
          poolConfig.setSoftMinEvictableIdleTimeMillis(30000);
          GenericObjectPool connectionPool =
              new GenericObjectPool(poolableConnectionFactory, poolConfig);
          poolableConnectionFactory.setPool(connectionPool);
          poolableConnectionFactory.setDefaultAutoCommit(false);
          poolableConnectionFactory.setDefaultReadOnly(false);
          return new PoolingDataSource(connectionPool);
        });
  }
}