package com.google.migration.dto.session;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/** NameAndCols object to store Spanner table name and column name mapping information. */
public class NameAndCols implements Serializable {

  /** Represents the name/id of the table. */
  private final String name;

  /** Mapping the column names/Ids. */
  private final Map<String, String> cols;

  public NameAndCols(String name, Map<String, String> cols) {
    this.name = name;
    this.cols = cols;
  }

  public String getName() {
    return name;
  }

  public Map<String, String> getCols() {
    return cols;
  }

  public String toString() {
    return String.format("{ 'name': '%s', 'cols': '%s' }", name, cols);
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof NameAndCols)) {
      return false;
    }
    final NameAndCols other = (NameAndCols) o;
    return this.name.equals(other.name) && this.cols.equals(other.cols);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, cols);
  }
}

