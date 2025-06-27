/*
 Copyright 2024 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

package com.google.migration.dofns;

import com.google.migration.Helpers;
import com.google.migration.dto.HashResult;
import com.google.migration.dto.PartitionRange;
import com.google.migration.dto.TableSpec;
import com.google.migration.partitioning.UUIDHelpers;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;

public class MapWithRangeFn extends DoFn<HashResult, KV<String, HashResult>> {
  static Comparator<PartitionRange> uuidPartitionRangeComparator = (o1, o2) -> {
    BigInteger lhs = UUIDHelpers.uuidToBigInt(UUID.fromString(o1.getStartRange()));
    BigInteger rhs = Helpers.uuidToBigInt(UUID.fromString(o2.getStartRange()));
    return lhs.compareTo(rhs);
  };

  static Comparator<PartitionRange> intPartitionRangeComparator = (o1, o2) -> {
    Integer lhs = Integer.parseInt(o1.getStartRange());
    Integer rhs = Integer.parseInt(o2.getStartRange());
    return lhs.compareTo(rhs);
  };

  static Comparator<PartitionRange> longPartitionRangeComparator = (o1, o2) -> {
    Long lhs = Long.parseLong(o1.getStartRange());
    Long rhs = Long.parseLong(o2.getStartRange());
    return lhs.compareTo(rhs);
  };

  static Comparator<PartitionRange> stringPartitionRangeComparator = (o1, o2) -> {
    String lhs = o1.getStartRange();
    String rhs = o2.getStartRange();
    return lhs.compareTo(rhs);
  };

  private PCollectionView<List<PartitionRange>> uuidRangesView;
  private List<PartitionRange> sortedPartitionRange = null;
  private MapWithRangeType mappingType;
  private String rangeFieldType = TableSpec.UUID_FIELD_TYPE;

  public MapWithRangeFn(PCollectionView<List<PartitionRange>> uuidRangesViewIn) {
    uuidRangesView = uuidRangesViewIn;
    mappingType = MapWithRangeType.JUST_RANGE;
  }

  public MapWithRangeFn(PCollectionView<List<PartitionRange>> uuidRangesViewIn,
      MapWithRangeType mappingTypeIn) {
    uuidRangesView = uuidRangesViewIn;
    mappingType = mappingTypeIn;
  }

  public MapWithRangeFn(PCollectionView<List<PartitionRange>> uuidRangesViewIn,
      MapWithRangeType mappingTypeIn,
      String rangeFieldTypeIn) {
    uuidRangesView = uuidRangesViewIn;
    mappingType = mappingTypeIn;
    rangeFieldType = rangeFieldTypeIn;
  }

  @Setup
  public void Setup() {
    sortedPartitionRange = null;
  }

  @Teardown
  public void Teardown() {
    sortedPartitionRange = null;
  }

  @ProcessElement
  public void processElement(ProcessContext c) {

    if(sortedPartitionRange == null) {
      List<PartitionRange> rangeList = c.sideInput(uuidRangesView);
      initializeSortedPartitionRange(rangeList);
    }

    HashResult result = c.element();
    PartitionRange rangeForRecord = getPartitionRangeForRecord(result);

    String key = String.format("%s|%s",
        rangeForRecord.getStartRange(),
        rangeForRecord.getEndRange());
    HashResult resultOut = new HashResult(result.key,
        result.isSource,
        result.origValue,
        result.sha256,
        result.timestampThresholdValue);
    resultOut.range = key;

    switch(mappingType) {
      case RANGE_PLUS_HASH:
        key = String.format("%s|%s|%s",
            rangeForRecord.getStartRange(),
            rangeForRecord.getEndRange(),
            result.sha256);
        break;
      case RANGE_PLUS_KEY_PLUS_HASH:
        key = String.format("%s|%s|%s|%s",
            rangeForRecord.getStartRange(),
            rangeForRecord.getEndRange(),
            result.key,
            result.sha256);
        break;
      case JUST_RANGE:
      default:
        break;
    }

    KV<String, HashResult> outVal = KV.of(key, resultOut);

    c.output(outVal);
  }

  public void initializeSortedPartitionRange(List<PartitionRange> rangeList) {
    Comparator<PartitionRange> comparator;
    switch(rangeFieldType) {
      case TableSpec.UUID_FIELD_TYPE:
        comparator = uuidPartitionRangeComparator;
        break;
      case TableSpec.INT_FIELD_TYPE:
        comparator = intPartitionRangeComparator;
        break;
      case TableSpec.LONG_FIELD_TYPE:
        comparator = longPartitionRangeComparator;
        break;
      case TableSpec.TIMESTAMP_FIELD_TYPE:
      case TableSpec.STRING_FIELD_TYPE:
        comparator = stringPartitionRangeComparator;
        break;
      default:
        throw new RuntimeException("Unable to determine comparator");
    }

    sortedPartitionRange = new ArrayList(rangeList);
    sortedPartitionRange.sort(comparator);
  }

  public PartitionRange getPartitionRangeForRecord(HashResult result) {
    switch(rangeFieldType) {
      case TableSpec.UUID_FIELD_TYPE:
        return getRangeFromList(result.key,
            uuidPartitionRangeComparator);
      case TableSpec.INT_FIELD_TYPE:
        return getRangeFromList(result.key,
            intPartitionRangeComparator);
      case TableSpec.LONG_FIELD_TYPE:
        return getRangeFromList(result.key,
            longPartitionRangeComparator);
      case TableSpec.TIMESTAMP_FIELD_TYPE:
      case TableSpec.STRING_FIELD_TYPE:
        return getRangeFromList(result.key,
            stringPartitionRangeComparator);
      default:
        break;
    }

    throw new RuntimeException(String.format("Unrecognized rangeFieldType (%s) in "
        + "MapWithRangeFn.getPartitionRangeForRecord", rangeFieldType));
  }

  private PartitionRange getRangeFromList(String valueToGetRangeFor,
      Comparator<PartitionRange> comparator) {

    int searchIndex =
        Collections.binarySearch(sortedPartitionRange,
            new PartitionRange(valueToGetRangeFor.toString(), valueToGetRangeFor.toString()),
            comparator);

    int rangeIndex = searchIndex;
    if(searchIndex < 0) rangeIndex = -searchIndex - 2;

    return sortedPartitionRange.get(rangeIndex);
  }

  public enum MapWithRangeType {
    JUST_RANGE,
    RANGE_PLUS_HASH,
    RANGE_PLUS_KEY_PLUS_HASH
  } // enum MapWithRangeType
} // class MapWithRangeFn