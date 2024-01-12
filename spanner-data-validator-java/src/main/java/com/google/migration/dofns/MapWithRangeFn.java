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

  private PCollectionView<List<PartitionRange>> uuidRangesView;
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

  @ProcessElement
  public void processElement(ProcessContext c) {
    List<PartitionRange> siBRanges = c.sideInput(uuidRangesView);

    HashResult result = c.element();
    PartitionRange rangeForRecord = getPartitionRangeForRecord(result, siBRanges);

    String key = String.format("%s|%s",
        rangeForRecord.getStartRange(),
        rangeForRecord.getEndRange());
    HashResult resultOut = new HashResult(result.key,
        result.isSource,
        result.origValue,
        result.sha256);
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

  public PartitionRange getPartitionRangeForRecord(HashResult result,
      List<PartitionRange> siBRanges) {
    switch(rangeFieldType) {
      case TableSpec.UUID_FIELD_TYPE:
        return getRangeFromList(result.key,
            siBRanges,
            uuidPartitionRangeComparator);
      case TableSpec.INT_FIELD_TYPE:
        return getRangeFromList(result.key,
            siBRanges,
            intPartitionRangeComparator);
      default:
        break;
    }

    throw new RuntimeException(String.format("Unrecognized rangeFieldType (%s) in "
        + "MapWithRangeFn.getPartitionRangeForRecord", rangeFieldType));
  }

  private PartitionRange getRangeFromList(String valueToGetRangeFor,
      List<PartitionRange> rangeList,
      Comparator<PartitionRange> comparator) {
    List<PartitionRange> sortedCopy = new ArrayList(rangeList);
    sortedCopy.sort(comparator);

    int searchIndex =
        Collections.binarySearch(sortedCopy,
            new PartitionRange(valueToGetRangeFor.toString(), valueToGetRangeFor.toString()),
            comparator);

    int rangeIndex = searchIndex;
    if(searchIndex < 0) rangeIndex = -searchIndex - 2;

    return sortedCopy.get(rangeIndex);
  }

  public enum MapWithRangeType {
    JUST_RANGE,
    RANGE_PLUS_HASH,
    RANGE_PLUS_KEY_PLUS_HASH
  } // enum MapWithRangeType
} // class MapWithRangeFn