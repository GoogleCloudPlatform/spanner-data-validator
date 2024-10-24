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

package com.google.migration;

import com.google.migration.common.DVTOptionsCore;
import com.google.migration.dto.ShardSpec;
import com.google.migration.dto.ShardSpecJsonDef;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShardSpecList {
  private static final Logger LOG = LoggerFactory.getLogger(ShardSpecList.class);

  public static List<ShardSpec> getShardSpecs(DVTOptionsCore options) {
    ArrayList<ShardSpec> shardSpecs = new ArrayList<>();

    String host = options.getServer();
    String user = options.getUsername();
    String pass = options.getPassword();
    String db = options.getSourceDB();

    ShardSpec spec = new ShardSpec(host,
        user,
        pass,
        String.format("%s", db),
        String.format("id%d", 1),
        0);
    shardSpecs.add(spec);

    spec = new ShardSpec(host,
        user,
        pass,
        String.format("%s%d", db, 2),
        String.format("id%d", 2),
        1);
    shardSpecs.add(spec);

    return shardSpecs;
  }

  public static List<ShardSpec> getShardSpecsFromJsonResource(String resourceFilename,
      Boolean verboseLogging) {
    ShardSpecJsonDef ssDef = ShardSpecJsonDef.fromJsonResourceFile(resourceFilename);

    return getShardSpecsFromShardSpecJsonDef(ssDef, verboseLogging);
  }

  public static List<ShardSpec> getShardSpecsFromJsonFile(String projectId, String jsonFile, Boolean verboseLogging) {
    ShardSpecJsonDef ssDef = ShardSpecJsonDef.fromJsonFile(projectId, jsonFile);

    return getShardSpecsFromShardSpecJsonDef(ssDef, verboseLogging);
  }

  private static List<ShardSpec> getShardSpecsFromShardSpecJsonDef(ShardSpecJsonDef ssDef,
      Boolean verboseLogging) {
    ArrayList<ShardSpec> shardSpecs = new ArrayList<>();

    Integer hostCount = ssDef.getHostCount();
    Integer shardCount = ssDef.getShardCount();
    Integer shardsPerHost = shardCount/hostCount;

    String hostDigitFormat = "%s%0" + ssDef.getHostnameSuffixDigits() + "d";
    String shardDigitFormat = "%s%0" + ssDef.getShardSuffixDigits() + "d";

    for(int i = 0; i < hostCount; i++) {
      String hostname = String.format(hostDigitFormat,
          ssDef.getHostnamePrefix(),
          ssDef.getHostnameSuffixStart() + i);

      String shardStaticSuffix = ssDef.getShardStaticSuffix();
      if(!Helpers.isNullOrEmpty(shardStaticSuffix)) {
        hostname = String.format("%s%s", hostname, shardStaticSuffix);
      }

      for(int j = 0; j < shardsPerHost; j++) {

        String db = String.format(shardDigitFormat,
            ssDef.getDbNamePrefix(),
            ssDef.getShardSuffixStart() + j + (i * shardsPerHost));
        ShardSpec spec = new ShardSpec(hostname,
            ssDef.getUsername(),
            ssDef.getPassword(),
            db,
            String.valueOf((i * shardsPerHost) + j),
            (i * shardsPerHost) + j);

        if(verboseLogging) {
          LOG.info(String.format("Hostname: %s, shard (db): %s", spec.getHost(), spec.getDb()));
        }

        shardSpecs.add(spec);
      } // for
    }

    return shardSpecs;
  }
} // class