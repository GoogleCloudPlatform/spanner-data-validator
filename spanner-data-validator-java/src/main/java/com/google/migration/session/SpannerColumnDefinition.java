package com.google.migration.session;

import java.io.Serializable;
import java.util.Objects;

/**
 * SpannerColumnDefinition object to store Spanner table name and column name mapping information.
 */
public class SpannerColumnDefinition implements Serializable {

  /** Represents the name of the Spanner column. */
  private final String name;

  /** Represents the type of the Spanner column. */
  private final SpannerColumnType t;

  public SpannerColumnDefinition(String name, SpannerColumnType type) {
    this.name = name;
    this.t = type;
  }

  public String getName() {
    return name;
  }

  public SpannerColumnType getType() {
    return t;
  }

  public String toString() {
    return String.format("{ 'name': '%s' , 'type': '%s'}", name, t);
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof SpannerColumnDefinition)) {
      return false;
    }
    final SpannerColumnDefinition other = (SpannerColumnDefinition) o;
    return this.name.equals(other.name) && this.t.equals(other.t);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, t);
  }
}

