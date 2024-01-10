package com.google.migration.dofns;

import com.google.migration.Helpers;
import com.google.migration.dto.HashResult;
import com.google.migration.dto.PartitionRange;
import com.google.migration.dto.TableSpec;
import com.google.migration.partitioning.UUIDHelpers;
import java.util.List;
import java.util.UUID;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;

public class MapWithRangeFn extends DoFn<HashResult, KV<String, HashResult>> {
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

  private PartitionRange getPartitionRangeForRecord(HashResult result,
      List<PartitionRange> siBRanges) {
    switch(rangeFieldType) {
      case TableSpec.UUID_FIELD_TYPE:
        UUID recordUUID = UUID.fromString(result.key);
        return UUIDHelpers.getRangeFromList(recordUUID, siBRanges);
      default:
        break;
    }

    throw new RuntimeException(String.format("Unrecognized rangeFieldType (%s) in "
        + "MapWithRangeFn.getPartitionRangeForRecord", rangeFieldType));
  }

  public enum MapWithRangeType {
    JUST_RANGE,
    RANGE_PLUS_HASH,
    RANGE_PLUS_KEY_PLUS_HASH
  } // enum MapWithRangeType
} // class MapWithRangeFn