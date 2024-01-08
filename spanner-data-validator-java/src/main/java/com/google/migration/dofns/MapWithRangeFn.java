package com.google.migration.dofns;

import com.google.migration.Helpers;
import com.google.migration.dto.HashResult;
import java.util.List;
import java.util.UUID;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;

public class MapWithRangeFn extends DoFn<HashResult, KV<String, HashResult>> {
  private PCollectionView<List<KV<UUID, UUID>>> uuidRangesView;
  private MapWithRangeType mappingType;

  public MapWithRangeFn(PCollectionView<List<KV<UUID, UUID>>> uuidRangesViewIn) {
    uuidRangesView = uuidRangesViewIn;
    mappingType = MapWithRangeType.JUST_RANGE;
  }

  public MapWithRangeFn(PCollectionView<List<KV<UUID, UUID>>> uuidRangesViewIn,
      MapWithRangeType mappingTypeIn) {
    uuidRangesView = uuidRangesViewIn;
    mappingType = mappingTypeIn;
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    HashResult result = c.element();
    UUID recordUUID = UUID.fromString(result.key);
    List<KV<UUID, UUID>> siBRanges = c.sideInput(uuidRangesView);

    KV<UUID, UUID> rangeForRecord = Helpers.getRangeFromList(recordUUID, siBRanges);

    String key = String.format("%s|%s", rangeForRecord.getKey(), rangeForRecord.getValue());
    HashResult resultOut = new HashResult(result.key,
        result.isSource,
        result.origValue,
        result.sha256);
    resultOut.range = key;

    switch(mappingType) {
      case RANGE_PLUS_HASH:
        key = String.format("%s|%s|%s",
            rangeForRecord.getKey(),
            rangeForRecord.getValue(),
            result.sha256);
        break;
      case RANGE_PLUS_KEY_PLUS_HASH:
        key = String.format("%s|%s|%s|%s",
            rangeForRecord.getKey(),
            rangeForRecord.getValue(),
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

  public enum MapWithRangeType {
    JUST_RANGE,
    RANGE_PLUS_HASH,
    RANGE_PLUS_KEY_PLUS_HASH
  } // enum MapWithRangeType
} // class MapWithRangeFn