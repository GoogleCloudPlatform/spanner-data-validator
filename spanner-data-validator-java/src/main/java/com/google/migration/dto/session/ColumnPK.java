package com.google.migration.dto.session;

import java.io.Serializable;
import java.util.Objects;

/** ColumnPK object to store table PK for both Spanner and Source. */
public class ColumnPK implements Serializable {

  /** Represents the name of the Spanner column. */
  private final String colId;

  /** Represents the position of the col in the primary key. */
  private final int order;

  public ColumnPK(String colId, int order) {
    this.colId = colId;
    this.order = order;
  }

  public String getColId() {
    return colId;
  }

  public int getOrder() {
    return order;
  }

  public String toString() {
    return String.format("{ 'colId': '%s' , 'order': '%s' }", colId, order);
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof ColumnPK)) {
      return false;
    }
    final ColumnPK other = (ColumnPK) o;
    return this.colId.equals(other.colId) && this.order == other.order;
  }

  @Override
  public int hashCode() {
    return Objects.hash(colId, order);
  }
}

