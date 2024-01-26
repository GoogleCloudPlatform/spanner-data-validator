package com.google.migration;

import com.google.migration.common.DVTOptionsCore;
import com.google.migration.dto.ShardSpec;
import com.google.migration.dto.ShardSpecJsonDef;
import java.util.ArrayList;
import java.util.List;

public class ShardSpecList {
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

  public static List<ShardSpec> getShardSpecsFromJsonFile(String resourceFilename) {
    ArrayList<ShardSpec> shardSpecs = new ArrayList<>();

    ShardSpecJsonDef ssDef = ShardSpecJsonDef.fromJsonResourceFile(resourceFilename);

    Integer hostCount = ssDef.getHostCount();
    Integer shardCount = ssDef.getShardCount();
    Integer shardsPerHost = shardCount/hostCount;

    String hostDigitFormat = "%s%0" + ssDef.getHostnameSuffixDigits() + "d";
    String shardDigitFormat = "%s%0" + ssDef.getShardSuffixDigits() + "d";

    for(int i = 0; i < hostCount; i++) {
      String hostname = String.format(hostDigitFormat,
          ssDef.getHostnamePrefix(),
          ssDef.getHostnameSuffixStart() + i);

      for(int j = 0; j < shardsPerHost; j++) {

        String db = String.format(shardDigitFormat,
            ssDef.getDbNamePrefix(),
            ssDef.getShardSuffixStart() + j);
        ShardSpec spec = new ShardSpec(hostname,
            ssDef.getUsername(),
            ssDef.getPassword(),
            db,
            String.valueOf((i * shardsPerHost) + j),
            (i * shardsPerHost) + j);
        shardSpecs.add(spec);
      } // for
    }

    return shardSpecs;
  }
} // class