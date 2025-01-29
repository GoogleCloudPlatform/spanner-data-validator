package com.google.migration.dto.session;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

public class Index implements Serializable {

  private final String name;

  private final String tableId;

  private final boolean unique;

  private final IndexKey[] keys;

  private final String id;

  private final String[] storedColumnIds;

  public Index(String name, String tableId, boolean unique, IndexKey[] keys, String id,
      String[] storedColumnIds) {
    this.name = name;
    this.tableId = tableId;
    this.unique = unique;
    this.keys = keys;
    this.id = id;
    this.storedColumnIds = storedColumnIds;
  }

  public String getName() {
    return name;
  }

  public String getTableId() {
    return tableId;
  }

  public boolean isUnique() {
    return unique;
  }

  public IndexKey[] getKeys() {
    return keys;
  }

  public String getId() {
    return id;
  }

  public String[] getStoredColumnIds() {
    return storedColumnIds;
  }

  @Override
  public String toString() {
    return "Index{" +
        "name='" + name + '\'' +
        ", tableId='" + tableId + '\'' +
        ", unique=" + unique +
        ", keys=" + Arrays.toString(keys) +
        ", id='" + id + '\'' +
        ", storedColumnIds=" + Arrays.toString(storedColumnIds) +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof Index)) {
      return false;
    }
    Index index = (Index) o;
    return unique == index.unique && Objects.equals(name, index.name)
        && Objects.equals(tableId, index.tableId) && Objects.deepEquals(keys,
        index.keys) && Objects.equals(id, index.id) && Objects.deepEquals(
        storedColumnIds, index.storedColumnIds);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, tableId, unique, Arrays.hashCode(keys), id,
        Arrays.hashCode(storedColumnIds));
  }
}
