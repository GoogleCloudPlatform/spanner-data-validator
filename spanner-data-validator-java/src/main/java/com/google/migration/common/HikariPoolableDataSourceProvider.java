package com.google.migration.common;

import com.zaxxer.hikari.HikariDataSource;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class HikariPoolableDataSourceProvider implements SerializableFunction<Void, DataSource> {
  private static final ConcurrentHashMap<String, DataSource> instances =
      new ConcurrentHashMap<>();

  private final String driverClassName;
  private final String username;
  private final String password;
  private final String jdbcUrl;
  private final Integer maxConnections;

  private HikariPoolableDataSourceProvider(String jdbcUrlIn,
      String usernameIn,
      String passwordIn,
      String driverClassNameIn,
      Integer maxConnectionsIn) {
    this.driverClassName = driverClassNameIn;
    this.username = usernameIn;
    this.password = passwordIn;
    this.jdbcUrl = jdbcUrlIn;
    this.maxConnections = maxConnectionsIn;
  }

  public static SerializableFunction<Void, DataSource> of(String jdbcUrlIn,
      String usernameIn,
      String passwordIn,
      String driverClassNameIn,
      Integer maxConnectionsIn) {
    return new HikariPoolableDataSourceProvider(jdbcUrlIn,
        usernameIn,
        passwordIn,
        driverClassNameIn,
        maxConnectionsIn);
  }

  @Override
  public DataSource apply(Void input) {
    return instances.computeIfAbsent(
        this.jdbcUrl,
        ignored -> {

          HikariDataSource ds = new HikariDataSource();
          ds.setJdbcUrl(this.jdbcUrl);
          ds.setUsername(this.username);
          ds.setPassword(this.password);
          ds.setDriverClassName(this.driverClassName);

          ds.setMaximumPoolSize(maxConnections);
          ds.setKeepaliveTime(30000);
          ds.setMaxLifetime(31000);
          ds.setConnectionTimeout(1000 * 3600 * 3);

          return ds;
        });
  }
}