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

package com.google;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.migration.Helpers;
import io.grpc.LoadBalancer.Helper;
import java.math.BigInteger;
import java.util.UUID;
import org.apache.beam.sdk.values.KV;
import org.junit.Test;

public class BigIntUUIDTest {
  @Test
  public void zeroTest() throws Exception {
    BigInteger val = BigInteger.ZERO;
    UUID zeroUUID = Helpers.bigIntToUUID(val);
    assertEquals(zeroUUID, new UUID(0, 0));
  }

  @Test
  public void oneTest() throws Exception {
    BigInteger val = BigInteger.ONE;
    UUID zeroUUID = Helpers.bigIntToUUID(val);
    assertEquals(new UUID(0, 1), zeroUUID);
  }

  @Test
  public void lowNumberTest() throws Exception {
    BigInteger val = BigInteger.valueOf(1000000);
    UUID zeroUUID = Helpers.bigIntToUUID(val);
    assertEquals(new UUID(0, 1000000), zeroUUID);
  }

  @Test
  public void longMaxTest() throws Exception {
    BigInteger val = BigInteger.valueOf(Long.MAX_VALUE);
    UUID zeroUUID = Helpers.bigIntToUUID(val);
    assertEquals(new UUID(0, Long.MAX_VALUE), zeroUUID);
  }

  @Test
  public void longMaxPlusTest() throws Exception {
    BigInteger val = BigInteger.valueOf(Long.MAX_VALUE);
    val = val.add(BigInteger.ONE);
    String valHex = val.toString(16);
    UUID calcUUID = Helpers.bigIntToUUID(val);
    assertEquals(UUID.fromString("00000000-0000-0000-8000-000000000000"), calcUUID);
  }

  @Test
  public void uuidToBigIntTest() throws Exception {
    //UUID uuidMax = UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff");
    UUID uuid = UUID.randomUUID();
    BigInteger uuidToBigInt = Helpers.uuidToBigInt(uuid);

    System.out.println(String.format("Orig: %s. Bigint: %d. Converted: %s", uuid, uuidToBigInt, Helpers.bigIntToUUID(uuidToBigInt)));
  }

  @Test
  public void inRangeTestTest() throws Exception {
    BigInteger bigMax = new BigInteger("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", 16);
    BigInteger val = BigInteger.ZERO;
    UUID zeroUUID = Helpers.bigIntToUUID(val);
    UUID oneUUID = Helpers.bigIntToUUID(BigInteger.ONE);

    // check if UUID(0) in range [UUID(0), UUID(1)] - should be true
    boolean result = Helpers.isUUIDInRange(zeroUUID, KV.of(zeroUUID, oneUUID));
    assertTrue(result);

    // check if UUID(1) in range [UUID(0), UUID(1)] - should be false
    result = Helpers.isUUIDInRange(oneUUID, KV.of(zeroUUID, oneUUID));
    assertFalse(result);

    // check if UUID(1) in range [UUID(0), UUID(1)] - should be false
    UUID rangeStartUUID = UUID.randomUUID();
    UUID rangeEndUUID = Helpers.bigIntToUUID(Helpers.uuidToBigInt(rangeStartUUID).add(BigInteger.valueOf(1000L)));
    UUID valueToCheckUUID = Helpers.bigIntToUUID(Helpers.uuidToBigInt(rangeStartUUID).add(BigInteger.valueOf(1L)));
    result = Helpers.isUUIDInRange(valueToCheckUUID, KV.of(rangeStartUUID, rangeEndUUID));
    assertTrue(result);
  }

  @Test
  public void scaleTest() throws Exception {
    BigInteger bigMax = new BigInteger("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", 16);
    BigInteger intMax = BigInteger.valueOf(Integer.MAX_VALUE);

    BigInteger scale = bigMax.divide(intMax);
    System.out.println(String.format("Scale: %s", scale));
  }

  @Test
  public void uuidMaxTest() throws Exception {
    BigInteger bigMax = new BigInteger("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", 16);

    System.out.println(String.format("UUID Max: %s", bigMax));
  }
} // class BigIntUUIDTest