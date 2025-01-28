package com.google.migration.session;

import java.io.Serializable;
import java.util.Objects;

/** SpannerColumnType object to store Source column type. */
public class SpannerColumnType implements Serializable {

  /** Represents the name of the Spanner column. */
  private final String name;

  /** Represents if the column is an array. */
  private final Boolean isArray;

  public SpannerColumnType(String name, Boolean isArray) {
    this.name = name;
    this.isArray = isArray;
  }

  public String getName() {
    return name;
  }

  public Boolean getIsArray() {
    return isArray;
  }

  public String toString() {
    return String.format("{ 'name': '%s' , 'isArray' :  '%s' }", name, isArray);
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof SpannerColumnType)) {
      return false;
    }
    final SpannerColumnType other = (SpannerColumnType) o;
    return this.name.equals(other.name) && this.isArray.equals(other.isArray);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, isArray);
  }
}

