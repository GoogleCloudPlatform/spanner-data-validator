package com.google.migration.composite;

import com.google.migration.dto.ComparerResult;
import com.google.migration.dto.HashResult;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

public class GetMatchCountDetailsTransform extends
    PTransform<PCollection<HashResult>, PCollection<ComparerResult>> {

  @Override
  public PCollection<ComparerResult> expand(PCollection<HashResult> input) {
    return null;
  }
} // class GetMatchCountDetailsTransform