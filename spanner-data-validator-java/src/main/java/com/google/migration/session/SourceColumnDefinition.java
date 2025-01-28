package com.google.migration.session;

import java.io.Serializable;
import java.util.Objects;

/**
 * SourceColumnDefinition object to store Spanner table name and column name mapping information.
 */
public class SourceColumnDefinition implements Serializable {

  /** Represents the name of the Source column. */
  private final String name;

  /** Represents the type of the Source column. */
  private final SourceColumnType type;

  public SourceColumnDefinition(String name, SourceColumnType type) {
    this.name = name;
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public SourceColumnType getType() {
    return type;
  }

  public String toString() {
    return String.format("{ 'name': '%s' , 'type': '%s'}", name, type);
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof SourceColumnDefinition)) {
      return false;
    }
    final SourceColumnDefinition other = (SourceColumnDefinition) o;
    return this.name.equals(other.name) && this.type.equals(other.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type);
  }
}

