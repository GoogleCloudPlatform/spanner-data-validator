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

package com.google.migration.composite;

import com.google.migration.Helpers;
import com.google.migration.dto.ComparerResult;
import com.google.migration.dto.HashResult;
import java.util.List;
import java.util.UUID;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CalculateMatchDetailsFn extends
    PTransform<PCollection<HashResult>, PCollection<ComparerResult>> {

  private static final Logger LOG = LoggerFactory.getLogger(CalculateMatchDetailsFn.class);

  private PCollectionView<List<KV<UUID, UUID>>> uuidRangesView;

  public CalculateMatchDetailsFn(PCollectionView<List<KV<UUID, UUID>>> uuidRangesViewIn) {
    uuidRangesView = uuidRangesViewIn;
  }

  @Override
  public PCollection<ComparerResult> expand(PCollection<HashResult> input) {
    PCollection<KV<String, HashResult>> mappedRecords =
        input.apply("Map records In", ParDo.of(new DoFn<HashResult, KV<String, HashResult>>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            HashResult result = c.element();
            UUID recordUUID = UUID.fromString(result.key);
            List<KV<UUID, UUID>> siBRanges = c.sideInput(uuidRangesView);

            KV<UUID, UUID> rangeForRecord = Helpers.getRangeFromList(recordUUID, siBRanges);

            KV<String, HashResult> outVal =
                KV.of(String.format("%s|%s", rangeForRecord.getKey(), rangeForRecord.getValue()),
                    result);
            c.output(outVal);
          }
        }).withSideInputs(uuidRangesView));

    mappedRecords.apply(Count.perKey());

    return null;
  }
} // class CalculateMatchDetailsFn