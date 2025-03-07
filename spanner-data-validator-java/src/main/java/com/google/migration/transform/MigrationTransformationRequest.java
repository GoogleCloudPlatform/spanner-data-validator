package com.google.migration.transform;

import java.util.Map;

public class MigrationTransformationRequest {
  private String tableName;
  private Map<String, Object> requestRow;
  private String shardId;
  private String eventType;

  public String getTableName() {
    return tableName;
  }

  public Map<String, Object> getRequestRow() {
    return requestRow;
  }

  public String getShardId() {
    return shardId;
  }

  public String getEventType() {
    return eventType;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public void setRequestRow(Map<String, Object> requestRow) {
    this.requestRow = requestRow;
  }

  public void setShardId(String shardId) {
    this.shardId = shardId;
  }

  public void setEventType(String eventType) {
    this.eventType = eventType;
  }

  public MigrationTransformationRequest(
      String tableName, Map<String, Object> requestRow, String shardId, String eventType) {
    this.tableName = tableName;
    this.requestRow = requestRow;
    this.shardId = shardId;
    this.eventType = eventType;
  }

  @Override
  public String toString() {
    return "MigrationTransformationRequest{"
        + "tableName="
        + tableName
        + ", requestRow="
        + requestRow
        + ", shardId="
        + shardId
        + ", eventType="
        + eventType
        + '}';
  }
}
