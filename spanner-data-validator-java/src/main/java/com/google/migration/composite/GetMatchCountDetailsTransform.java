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