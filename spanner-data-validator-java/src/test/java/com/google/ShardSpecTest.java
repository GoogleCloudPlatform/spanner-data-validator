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

import com.google.migration.ShardSpecList;
import com.google.migration.dto.ShardSpec;
import java.util.List;
import org.junit.Test;

public class ShardSpecTest {
  @Test
  public void shardSpecReadFromJsonTest() throws Exception {
    List<ShardSpec> sList =
        ShardSpecList
            .getShardSpecsFromJsonResource("json/shard-spec-sample-v1.json",
                true);

    assertEquals(16, sList.size());
    assertEquals("testhost-001.local.com", sList.get(0).getHost());
    assertEquals("test-0000", sList.get(0).getDb());
    assertEquals("test-0007", sList.get(7).getDb());

    assertEquals("testhost-002.local.com", sList.get(15).getHost());
    assertEquals("test-0008", sList.get(8).getDb());
    assertEquals("test-0015", sList.get(15).getDb());
  }

  @Test
  public void singleShardSpecTest() throws Exception {
    List<ShardSpec> sList =
        ShardSpecList
            .getShardSpecsFromJsonResource("json/shard-spec-single-shard-sample.json",
                true);

    assertEquals(1, sList.size());
    assertEquals("testhost-001", sList.get(0).getHost());
    assertEquals("test-0000", sList.get(0).getDb());
  }
} // class