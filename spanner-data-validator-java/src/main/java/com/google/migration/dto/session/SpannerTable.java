package com.google.migration.dto.session;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.arrow.flatbuf.Bool;

/** SpannerTable object to store Spanner table name and column name mapping information. */
public class SpannerTable implements Serializable {

  /** Represents the name of the Spanner table. */
  private final String name;

  /** List of all the column IDs in the same order as in the Spanner table. */
  private final String[] colIds;

  /** Maps the column ID to the column definition. */
  private final Map<String, SpannerColumnDefinition> colDefs;

  private final ColumnPK[] primaryKeys;

  private final Index[] indexes;

  /** Points to the id of the sharded column. Will be null for non-sharded migrations */
  private final String shardIdColumn;

  public SpannerTable(
      String name,
      String[] colIds,
      Map<String, SpannerColumnDefinition> colDefs,
      ColumnPK[] primaryKeys, Index[] indexes,
      String shardIdColumn) {
    this.name = name;
    this.colIds = (colIds == null) ? (new String[] {}) : colIds;
    this.colDefs = (colDefs == null) ? (new HashMap<String, SpannerColumnDefinition>()) : colDefs;
    this.primaryKeys = (primaryKeys == null) ? (new ColumnPK[] {}) : primaryKeys;
    this.indexes = indexes;
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

  public String getSpannerQuery(String partitionKeyColId,
      String[] sourceColIds,
      Boolean isCustomTransformation,
      Boolean includeBackTicks) {
    //find the common colIds between colIds field and the param sourceColIds and sort that to use in rest of the code
    String[] commonColIds = Arrays.stream(colIds).filter(x -> Arrays.asList(sourceColIds).contains(x)).toArray(String[]::new);
    Arrays.sort(commonColIds);
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT ");
    //add the partition key first
    sb.append(prependTableNameToColumn(name, colDefs.get(partitionKeyColId).getName(), includeBackTicks))
        .append(",");
    //add the rest of the cols
    for (String colId : commonColIds) {
      if (!colId.equals(partitionKeyColId)) {
        sb.append(prependTableNameToColumn(name, colDefs.get(colId).getName(), includeBackTicks)).append(",");
      }
    }
    //add the custom transformation columns if custom transformations is enabled
    //There are two cases: an existing column is under transformation or a new one is added.
    //If an existing column is under transformation, the dst query does not get modified.
    //If a new column is added, the dst query gets modified. Below is how -
    // All non-common columns in spannerTable are assumed to be under custom transformation
    //by default. There is no other way to determine which columns are under custom transformation
    //from the session file. Custom transformations are always added alphabetically sorted
    //to the end of the query.
    if (isCustomTransformation) {
      String []customTransformColIds = Arrays.stream(colIds).filter(x -> !Arrays.asList(sourceColIds).contains(x)).toArray(String[]::new);
      if (customTransformColIds.length > 0) {
        Arrays.sort(customTransformColIds);
        for (String colId : customTransformColIds) {
          sb.append(prependTableNameToColumn(name, colDefs.get(colId).getName(), includeBackTicks)).append(",");
        }
      }
    }
    sb.deleteCharAt(sb.length() - 1);
    sb.append(" FROM ").append(name);
    sb.append(" WHERE ").append(prependTableNameToColumn(name, colDefs.get(partitionKeyColId).getName(), includeBackTicks))
        .append(" >= @p1 AND ").append(prependTableNameToColumn(name, colDefs.get(partitionKeyColId).getName(), includeBackTicks))
        .append(" <= @p2");
    return sb.toString();
  }

  private String prependTableNameToColumn(String tableName, String columnName, Boolean includeBackTicks) {
    if(includeBackTicks) {
      return "`" + tableName + "`" + ".`" + columnName + "`";
    } else {
      return tableName + "." + columnName;
    }
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

  public Index[] getIndexes() {
    return indexes;
  }
}

