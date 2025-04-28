package com.google.migration;

import com.google.migration.dto.HashResult;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.PCollection;

public class PipelineTracker {

  public Integer getTotalTablesSoFar() {
    return totalTablesSoFar;
  }

  public Integer getTrackedTablesSoFar() {
    return trackedTablesSoFar;
  }

  public Integer getMaxTablesInEffectAtOneTime() {
    return maxTablesInEffectAtOneTime;
  }

  public void setMaxTablesInEffectAtOneTime(Integer maxTablesInEffectAtOneTime) {
    this.maxTablesInEffectAtOneTime = maxTablesInEffectAtOneTime;
  }

  private List<PCollection<?>> lastSpannerReadList = new ArrayList<>();
  private List<PCollection<?>> lastJDBCReadList = new ArrayList<>();
  private Integer totalTablesSoFar = 0;
  private Integer trackedTablesSoFar = 0;
  private Integer maxTablesInEffectAtOneTime;

  public List<PCollection<?>> getLastSpannerReadList() {
    return lastSpannerReadList;
  }

  public boolean isPastMaxTablesInEffectAtOneTime() {
    return trackedTablesSoFar >= maxTablesInEffectAtOneTime;
  }

  public PCollection<?> applySpannerWait(PCollection<?> input) {
    if(isPastMaxTablesInEffectAtOneTime()) {
      String name = String.format("SpannerWaitSignal-%d",
          totalTablesSoFar / maxTablesInEffectAtOneTime);
      PCollection<?> retVal = input.apply(name, Wait.on(lastSpannerReadList));
      resetSpannerWaitList();

      return retVal;
    } else {
      return input;
    }
  }

  public PCollection<?> applyJDBCWait(PCollection<?> input) {
    if(isPastMaxTablesInEffectAtOneTime()) {
      String name = String.format("JDBCWaitSignal-%d",
          totalTablesSoFar / maxTablesInEffectAtOneTime);
      PCollection<?> retVal = input.apply(name, Wait.on(lastJDBCReadList));
      resetJDBCWaitList();

      return retVal;
    } else {
      return input;
    }
  }

  public void addToJDBCReadList(PCollection<HashResult> jdbcRead) {
    totalTablesSoFar++;
    trackedTablesSoFar++;
    lastJDBCReadList.add(jdbcRead);
  }

  public void addToSpannerReadList(PCollection<HashResult> spannerRead) {
    lastSpannerReadList.add(spannerRead);
  }

  public void resetSpannerWaitList() {
    lastSpannerReadList = new ArrayList<>();
  }

  public void resetJDBCWaitList() {
    lastJDBCReadList = new ArrayList<>();
    trackedTablesSoFar = 0;
  }
}