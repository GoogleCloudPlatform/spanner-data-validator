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

package com.google.migration.partitioning;

import com.google.migration.dto.TableSpec;

public class PartitionRangeListFetcherFactory {

  public static PartitionRangeListFetcher getFetcher(String partitionType) {
    partitionType = partitionType.toUpperCase();

    switch(partitionType) {
      case TableSpec.UUID_FIELD_TYPE:
        return new UUIDPartitionRangeListFetcher();
      case TableSpec.INT_FIELD_TYPE:
        return new IntegerPartitionRangeListFetcher();
      case TableSpec.LONG_FIELD_TYPE:
        return new LongPartitionRangeListFetcher();
      case TableSpec.TIMESTAMP_FIELD_TYPE:
        return new TimestampPartitionRangeListFetcher();
      case TableSpec.STRING_FIELD_TYPE:
        return new StringPartitionListFetcher();
      default:
        throw new RuntimeException(String.format("Unrecognized partition type: %s", partitionType));
    } // switch
  }
} // class PartitionRangeListFetcherFactory