package com.google.migration.dto;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;

@DefaultCoder(AvroCoder.class)
public class ShardSpec {
  private String host;
  private String user;
  private String password;
  private String db;
  private String shardId;
  private Integer shardIndex;

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getDb() {
    return db;
  }

  public void setDb(String db) {
    this.db = db;
  }

  public String getShardId() {
    return shardId;
  }

  public void setShardId(String shardId) {
    this.shardId = shardId;
  }

  public Integer getShardIndex() {
    return shardIndex;
  }

  public void setShardIndex(Integer shardIndex) {
    this.shardIndex = shardIndex;
  }

  public ShardSpec() {
  }

  public ShardSpec(String hostIn,
      String userIn,
      String passIn,
      String dbIn,
      String shardId,
      Integer shardIndex) {
    host = hostIn;
    user = userIn;
    password = passIn;
    db = dbIn;
    this.shardId = shardId;
    this.shardIndex = shardIndex;
  }
} // class ShardSpec