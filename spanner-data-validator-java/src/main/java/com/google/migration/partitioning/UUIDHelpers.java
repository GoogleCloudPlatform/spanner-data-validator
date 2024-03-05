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

import java.math.BigInteger;
import java.util.UUID;

public class UUIDHelpers {
  public static UUID bigIntToUUID(BigInteger in) {
    long low = in.longValue();
    long high = in.shiftRight(64).longValue();

    return new UUID(high, low);
  }

  public static BigInteger uuidToBigInt(UUID in) {
    // https://stackoverflow.com/questions/55752927/how-to-convert-an-unsigned-long-to-biginteger
    final BigInteger MASK_64 =
        BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);

    BigInteger msb = BigInteger.valueOf(in.getMostSignificantBits()).and(MASK_64).shiftLeft(64);
    BigInteger lsb = BigInteger.valueOf(in.getLeastSignificantBits()).and(MASK_64);
    BigInteger retVal = msb.or(lsb);

    return retVal;
  }
} // class UUIDHelpers