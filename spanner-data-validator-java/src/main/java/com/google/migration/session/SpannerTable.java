package com.google.migration.session;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** SpannerTable object to store Spanner table name and column name mapping information. */
public class SpannerTable implements Serializable {

  /** Represents the name of the Spanner table. */
  private final String name;

  /** List of all the column IDs in the same order as in the Spanner table. */
  private final String[] colIds;

  /** Maps the column ID to the column definition. */
  private final Map<String, SpannerColumnDefinition> colDefs;

  private final ColumnPK[] primaryKeys;

  /** Points to the id of the sharded column. Will be null for non-sharded migrations */
  private final String shardIdColumn;

  public SpannerTable(
      String name,
      String[] colIds,
      Map<String, SpannerColumnDefinition> colDefs,
      ColumnPK[] primaryKeys,
      String shardIdColumn) {
    this.name = name;
    this.colIds = (colIds == null) ? (new String[] {}) : colIds;
    this.colDefs = (colDefs == null) ? (new HashMap<String, SpannerColumnDefinition>()) : colDefs;
    this.primaryKeys = (primaryKeys == null) ? (new ColumnPK[] {}) : primaryKeys;
    this.shardIdColumn = shardIdColumn;
  }

  public String getName() {
    return name;
  }

  public String[] getColIds() {
    return colIds;
  }

  public Map<String, SpannerColumnDefinition> getColDefs() {
    return colDefs;
  }

  public ColumnPK[] getPrimaryKeys() {
    return primaryKeys;
  }

  public Set<String> getPrimaryKeySet() {

    Set<String> response = new HashSet<>();
    if (primaryKeys != null && colDefs != null) {
      for (ColumnPK p : primaryKeys) {
        SpannerColumnDefinition pkColDef = colDefs.get(p.getColId());
        if (pkColDef != null) {
          response.add(pkColDef.getName());
        }
      }
    }
    return response;
  }

  public String getShardIdColumn() {
    return shardIdColumn;
  }

  public String getSpannerQuery() {
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT ");
    Arrays.sort(colIds);
    for (String colId : colIds) {
      sb.append(colDefs.get(colId).getName()).append(",");
    }
    sb.deleteCharAt(sb.length() - 1);
    sb.append(" FROM ").append(name);
    if (primaryKeys != null && primaryKeys.length > 0) {
      sb.append(" WHERE ").append(colDefs.get(primaryKeys[0].getColId()).getName()).append(" >= @p1 AND ").append(colDefs.get(primaryKeys[0].getColId()).getName()).append(" <= @p2");
    }
    return sb.toString();
  }

  public String toString() {
    String pvalues = "";
    if (primaryKeys != null) {
      for (int i = 0; i < primaryKeys.length; i++) {
        pvalues += primaryKeys[i].toString();
        pvalues += ",";
      }
    }
    return String.format(
        "{ 'name': '%s', colIds': '%s', 'colDefs': '%s','primaryKeys': '%s', shardIdColumn: '%s' }",
        name, Arrays.toString(colIds), colDefs, pvalues, shardIdColumn);
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof SpannerTable)) {
      return false;
    }
    final SpannerTable other = (SpannerTable) o;
    return this.name.equals(other.name)
        && Arrays.equals(this.colIds, other.colIds)
        && this.colDefs.equals(other.colDefs)
        && Arrays.equals(this.primaryKeys, other.primaryKeys)
        && Objects.equals(this.shardIdColumn, other.shardIdColumn);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        name, Arrays.hashCode(colIds), colDefs, Arrays.hashCode(primaryKeys), shardIdColumn);
  }
}

