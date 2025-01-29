package com.google.migration.dto.session;

import java.io.Serializable;
import java.util.Objects;

public class IndexKey implements Serializable {

  private final String colId;

  private final boolean desc;

  private final int order;

  public IndexKey(String colId, boolean desc, int order) {
    this.colId = colId;
    this.desc = desc;
    this.order = order;
  }

  public String getColId() {
    return colId;
  }

  public boolean isDesc() {
    return desc;
  }

  public int getOrder() {
    return order;
  }

  @Override
  public String toString() {
    return "IndexKey{" +
        "colId='" + colId + '\'' +
        ", desc=" + desc +
        ", order=" + order +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof IndexKey)) {
      return false;
    }
    IndexKey indexKey = (IndexKey) o;
    return desc == indexKey.desc && order == indexKey.order && Objects.equals(colId,
        indexKey.colId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(colId, desc, order);
  }
}
