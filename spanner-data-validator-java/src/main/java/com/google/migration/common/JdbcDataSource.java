package com.google.migration.common;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JdbcDataSource extends BasicDataSource implements Serializable {

  /*
   * Implementation Detail, we take required members of JDBCIOWrapperConfig as private members here since there are members of JDBCIOWrapperConfig like FluentBackoff which aren't marked serializable by Apache Beam.
   */

  private final String sourceDbURL;
  private final Integer maxConnections;
  private final Integer initialConnections;
  private final Integer maxIdleConnections;
  private final Long maxConnDurationInMillis;
  private final Long softMinEvictableIdleTimeMillis;
  private final Integer maxOpenStatements;
  private final String jdbcDriverClassName;
  private final Boolean defaultAutocommit;
  private final String username;
  private final String password;
  private static final Logger LOG = LoggerFactory.getLogger(JdbcDataSource.class);

  /**
   * Sets the {@code testOnBorrow} property. This property determines whether or not the pool will
   * validate objects before they are borrowed from the pool. Defaults to True.
   */
  private final Boolean testOnBorrow;

  /**
   * Sets the {@code testOnCreate} property. This property determines whether or not the pool will
   * validate objects immediately after they are created by the pool.
   */
  private final Boolean testOnCreate;

  /**
   * Sets the {@code testOnReturn} property. This property determines whether or not the pool will
   * validate objects before they are returned to the pool.
   */
  private final Boolean testOnReturn;

  /**
   * Sets the {@code testWhileIdle} property. This property determines whether or not the idle
   * object evictor will validate connections.
   */
  private final Boolean testWhileIdle;

  /** Sets the {@code validationQuery}. */
  private final String validationQuery;

  /**
   * The timeout in seconds before an abandoned connection can be removed.
   *
   * <p>Creating a Statement, PreparedStatement or CallableStatement or using one of these to
   * execute a query (using one of the execute methods) resets the lastUsed property of the parent
   * connection.
   *
   * <p>Abandoned connection cleanup happens when:
   *
   * <ul>
   *   <li>{@link BasicDataSource#getRemoveAbandonedOnBorrow()} or {@link
   *       BasicDataSource#getRemoveAbandonedOnMaintenance()} = true
   *   <li>{@link BasicDataSource#getNumIdle() numIdle} &lt; 2
   *   <li>{@link BasicDataSource#getNumActive() numActive} &gt; {@link
   *       BasicDataSource#getMaxTotal() maxTotal} - 3
   * </ul>
   *
   * <p>
   */
  private final Integer removeAbandonedTimeout;

  /**
   * The minimum amount of time an object may sit idle in the pool before it is eligible for
   * eviction by the idle object evictor.
   */
  private final Long minEvictableIdleTimeMillis;

  public JdbcDataSource(BasicDataSource dataSource) {
    this.sourceDbURL = dataSource.getUrl();
    this.username = dataSource.getUsername();
    this.password = dataSource.getPassword();
    this.maxConnections = dataSource.getMaxTotal();
    this.maxConnDurationInMillis = dataSource.getMaxConnLifetimeMillis();
    this.initialConnections = dataSource.getInitialSize();
    this.maxIdleConnections = dataSource.getMaxIdle();
    this.jdbcDriverClassName = dataSource.getDriverClassName();
    this.testOnBorrow = dataSource.getTestOnBorrow();
    this.testOnCreate = dataSource.getTestOnCreate();
    this.testOnReturn = dataSource.getTestOnReturn();
    this.testWhileIdle = dataSource.getTestWhileIdle();
    this.validationQuery = dataSource.getValidationQuery();
    this.removeAbandonedTimeout = dataSource.getRemoveAbandonedTimeout();
    this.minEvictableIdleTimeMillis = dataSource.getMinEvictableIdleTimeMillis();
    this.softMinEvictableIdleTimeMillis = dataSource.getSoftMinEvictableIdleTimeMillis();
    this.defaultAutocommit = dataSource.getDefaultAutoCommit();
    this.maxOpenStatements = dataSource.getMaxOpenPreparedStatements();
    this.initializeSuper();
  }

  private void initializeSuper() {

    super.setDriverClassName(jdbcDriverClassName);
    super.setUsername(username);
    super.setPassword(password);

    super.setUrl(sourceDbURL);

    if (maxConnections != null) {
      super.setMaxTotal(maxConnections.intValue());
    }
    super.setMaxTotal(maxConnections);
    super.setMaxIdle(maxIdleConnections);
    super.setMaxConnLifetimeMillis(maxConnDurationInMillis);
    super.setInitialSize(initialConnections);
    super.setTestOnBorrow(testOnBorrow);
    super.setTestOnCreate(testOnCreate);
    super.setTestOnReturn(testOnReturn);
    super.setTestWhileIdle(testWhileIdle);
    super.setValidationQuery(validationQuery);
    super.setRemoveAbandonedTimeout(removeAbandonedTimeout);
    super.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
    super.setSoftMinEvictableIdleTimeMillis(softMinEvictableIdleTimeMillis);
    super.setDefaultAutoCommit(defaultAutocommit);
    super.setMaxOpenPreparedStatements(maxOpenStatements);
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    // Call initializeSuper after deserialization
    initializeSuper();
  }

  @Override
  public String toString() {
    return String.format(
        "JdbcDataSource: {\"sourceDbURL\":\"%s\", "
            + "\"maxConnections\":\"%s\","
            + "\"maxIdleConnections\":\"%s\","
            + " }",
        sourceDbURL,
        maxConnections,
        maxIdleConnections);
  }
}
