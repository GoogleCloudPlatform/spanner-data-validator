package com.google.migration.transform;

import java.util.Map;

public class MigrationTransformationResponse {
  private Map<String, Object> responseRow;
  private boolean isEventFiltered;

  public Map<String, Object> getResponseRow() {
    return responseRow;
  }

  public void setResponseRow(Map<String, Object> responseRow) {
    this.responseRow = responseRow;
  }

  public boolean isEventFiltered() {
    return isEventFiltered;
  }

  public void setEventFiltered(boolean eventFiltered) {
    isEventFiltered = eventFiltered;
  }

  public MigrationTransformationResponse(Map<String, Object> responseRow, boolean isEventFiltered) {
    this.responseRow = responseRow;
    this.isEventFiltered = isEventFiltered;
  }

  @Override
  public String toString() {
    return "MigrationTransformationResponse{"
        + "responseRow="
        + responseRow
        + ", isEventFiltered="
        + isEventFiltered
        + '}';
  }
}
