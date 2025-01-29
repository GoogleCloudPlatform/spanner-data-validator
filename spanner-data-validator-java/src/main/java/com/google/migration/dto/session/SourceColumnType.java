package com.google.migration.dto.session;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/** SourceColumnType object to store Source column type. */
public class SourceColumnType implements Serializable {

  /** Represents the type of the Source column. */
  private final String name;

  /** Represents mods of the Source Column (ex: varchar(30) will have mods as [30]). */
  private final Long[] mods;

  /** Represents array bounds(if any) of the type. */
  private final Long[] arrayBounds;

  public SourceColumnType(String name, Long[] mods, Long[] arrayBounds) {
    this.name = name;
    this.mods = mods;
    this.arrayBounds = arrayBounds;
  }

  public String getName() {
    return name;
  }

  public Long[] getMods() {
    return mods;
  }

  public Long[] getArrayBounds() {
    return arrayBounds;
  }

  public String toString() {
    return String.format(
        "{ 'name': '%s' , 'mods' :  '%s', 'arrayBounds' :  '%s' }",
        name, Arrays.toString(mods), Arrays.toString(arrayBounds));
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof SourceColumnType)) {
      return false;
    }
    final SourceColumnType other = (SourceColumnType) o;
    return this.name.equals(other.name)
        && Arrays.equals(this.mods, other.mods)
        && Arrays.equals(this.arrayBounds, other.arrayBounds);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, Arrays.hashCode(mods), Arrays.hashCode(arrayBounds));
  }
}

