package com.google.migration.session;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** SourceTable object to store Source table name and column name mapping information. */
public class SourceTable implements Serializable {

  /** Represents the name of the Source table. */
  private final String name;

  /** Represents the name of the Source schema. */
  private final String schema;

  /** List of all the column IDs in the same order as in the Source table. */
  private final String[] colIds;

  /** Maps the column ID to the column definition. */
  private final Map<String, SourceColumnDefinition> colDefs;

  private final ColumnPK[] primaryKeys;

  public SourceTable(
      String name,
      String schema,
      String[] colIds,
      Map<String, SourceColumnDefinition> colDefs,
      ColumnPK[] primaryKeys) {
    this.name = name;
    this.schema = schema;
    this.colIds = (colIds == null) ? (new String[] {}) : colIds;
    this.colDefs = (colDefs == null) ? (new HashMap<String, SourceColumnDefinition>()) : colDefs;
    // We don't replace nulls with empty arrays as the session file for this field can contain null
    // values.
    this.primaryKeys = primaryKeys;
  }

  public String getName() {
    return name;
  }

  public String getSchema() {
    return schema;
  }

  public String[] getColIds() {
    return colIds;
  }

  public Map<String, SourceColumnDefinition> getColDefs() {
    return colDefs;
  }

  public ColumnPK[] getPrimaryKeys() {
    return primaryKeys;
  }

  public Set<String> getPrimaryKeySet() {

    Set<String> response = new HashSet<>();
    if (primaryKeys != null && colDefs != null) {
      for (ColumnPK p : primaryKeys) {
        SourceColumnDefinition pkColDef = colDefs.get(p.getColId());
        if (pkColDef != null) {
          response.add(pkColDef.getName());
        }
      }
    }
    return response;
  }

  public String getSourceQuery() {
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT ");
    Arrays.sort(colIds);
    for (String colId : colIds) {
      sb.append(colDefs.get(colId).getName()).append(",");
    }
    sb.deleteCharAt(sb.length() - 1);
    sb.append(" FROM ").append(name);
    if (primaryKeys != null && primaryKeys.length > 0) {
      sb.append(" WHERE ").append(colDefs.get(primaryKeys[0].getColId()).getName()).append(" >= ? AND ").append(colDefs.get(primaryKeys[0].getColId()).getName()).append(" <= ?");
    }
    return sb.toString();
  }

  public Integer getPrimaryKeyPositionInQuery() {
    Arrays.sort(colIds);
    return Arrays.asList(colIds).indexOf(primaryKeys[0].getColId());
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
        "{ 'name': '%s', 'schema': '%s', 'colIds': '%s', 'colDefs': '%s','primaryKeys': '%s' }",
        name, schema, Arrays.toString(colIds), colDefs, pvalues);
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof SourceTable)) {
      return false;
    }
    final SourceTable other = (SourceTable) o;
    return this.name.equals(other.name)
        && this.schema.equals(other.schema)
        && Arrays.equals(this.colIds, other.colIds)
        && this.colDefs.equals(other.colDefs)
        && Arrays.equals(this.primaryKeys, other.primaryKeys);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        name, schema, Arrays.hashCode(colIds), colDefs, Arrays.hashCode(primaryKeys));
  }
}
