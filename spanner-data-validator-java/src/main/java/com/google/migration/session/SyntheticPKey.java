package com.google.migration.session;

import java.io.Serializable;
import java.util.Objects;

/** SyntheticPKey object column information for synthetically added PKs. */
public class SyntheticPKey implements Serializable {

  /** Represents the name of the synthetic PK column. */
  private final String colId;

  /**
   * This is a field in the HarbourBridge session file used to generate PK values via bit-reversal.
   */
  private long sequence;

  public SyntheticPKey(String colId, long sequence) {
    this.colId = colId;
    this.sequence = sequence;
  }

  public String getColId() {
    return colId;
  }

  public long getSequence() {
    return sequence;
  }

  public String toString() {
    return String.format("{ 'colId': '%s', 'sequence': %d }", colId, sequence);
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof SyntheticPKey)) {
      return false;
    }
    final SyntheticPKey other = (SyntheticPKey) o;
    return this.colId.equals(other.colId) && this.sequence == other.sequence;
  }

  @Override
  public int hashCode() {
    return Objects.hash(colId, sequence);
  }
}

