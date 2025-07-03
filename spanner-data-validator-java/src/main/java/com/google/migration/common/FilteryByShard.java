package com.google.migration.common;

import com.google.cloud.spanner.Struct;
import com.google.migration.dto.HashResult;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
so the main logic to get shard Index is:
- if there is only one shard, return 0
- for most of the services, take ddrkey column, do a bit reverse then do mod operation
- for tradehousei service, if the custom column passed in then do mod operation shardedIdValue % ddrCount, otherwise, use ddrkey column to do calculation
- for gdb service, take a combination of "type" and "name", or "from" column to do special calculation
- if no custom column passed in and there is no ddrkey column either, then it would by default use shard 0. And then the actual transformation code when doing reverse replication will skip this row.
 */
@DefaultCoder(AvroCoder.class)
public class FilteryByShard {
  public static final String OTHER_SERVICE_NAME = "other";
  public static final String TRADEHOUSEI_SERVICE_NAME = "tradehousei";
  public static final String GDB_SERVICE_NAME = "gdb";
  public static final String DDR_KEY_COL_NAME = "ddrkey";
  private static final Logger LOG = LoggerFactory.getLogger(FilteryByShard.class);

  private String defaultDdrColumn = DDR_KEY_COL_NAME;
  private Long ddrCount = 1L;
  private String serviceName;
  private String tableName = "";
  private Boolean enableShardFiltering = false;

  public void setTableName(String tableNameIn) {
    tableName = tableNameIn;
  }

  public String getTableName() {
    return tableName;
  }

  public FilteryByShard() {
  }

  public FilteryByShard(Long ddrCountIn,
      String serviceNameIn,
      String tableNameIn,
      String shardIdCalcColName,
      Boolean enableShardFilteringIn) {
    enableShardFiltering = enableShardFilteringIn;
    ddrCount = ddrCountIn;
    tableName = tableNameIn;
    defaultDdrColumn = shardIdCalcColName;

    switch(serviceNameIn) {
      case GDB_SERVICE_NAME:
        serviceName =  GDB_SERVICE_NAME;
        break;
      case TRADEHOUSEI_SERVICE_NAME:
        serviceName =  TRADEHOUSEI_SERVICE_NAME;
        break;
      default:
        serviceName = OTHER_SERVICE_NAME;
    } // switch
  }

  public String getLogicalShardId(Struct spannerStruct,
      Boolean enableVerboseLogging) {
    if(!enableShardFiltering || ddrCount == 1) {
      if(enableVerboseLogging) {
        LOG.warn("'Setting logical shard to 0!. enableShardFiltering: {}, ddrCount:{}",
            enableShardFiltering,
            ddrCount);
      }
      return "";
    }

    Boolean colExists =
            spannerStruct.getType().getStructFields().stream().anyMatch(f -> f.getName().equals(defaultDdrColumn));

    if(serviceName.equals(OTHER_SERVICE_NAME)) {
      if(!colExists) {
        if(enableVerboseLogging) {
          LOG.warn("{} col does not exist; service name: {}",
              defaultDdrColumn,
              serviceName);
        }
        return "";
      }

      Long ddrKeyValue = spannerStruct.getLong(defaultDdrColumn);
      Long shardedIdValue = Long.reverse(ddrKeyValue);
      Long logicalShardId = shardedIdValue % this.ddrCount;
      return Long.toString(logicalShardId);
    }
    else if(serviceName.equals(TRADEHOUSEI_SERVICE_NAME)) {
      if(!colExists) {
        if(enableVerboseLogging) {
          LOG.warn("{} col does not exist; service name: {}",
              defaultDdrColumn,
              serviceName);
        }
        return "";
      }

      Long ddrKeyValue = spannerStruct.getLong(defaultDdrColumn);

      Long logicalShardId = ddrKeyValue % this.ddrCount;

      if(enableVerboseLogging) {
        LOG.warn("'Setting logical shard to {}. ddrKeyValue: {}; ddrCount: {}",
            logicalShardId,
            ddrKeyValue,
            ddrCount);
      }

      return Long.toString(logicalShardId);
    }
    else if(serviceName.equals(GDB_SERVICE_NAME)) {

      Long logicalShardId = 0L;
      if(tableName.contains("nodes")){
        String type = spannerStruct.getString("type");
        String name = spannerStruct.getString("name");
        String key = type + "?" + name;
        logicalShardId = hash(key) % this.ddrCount.longValue();
      } else if(tableName.contains("edges")) {
        String from = spannerStruct.getString("from");
        String[] fromSplits = from.split("\\?");
        String key = fromSplits[0] + "?" + fromSplits[1];
        logicalShardId = hash(key) % this.ddrCount.longValue();
      } else {
        return "";
      }

      return Long.toString(logicalShardId);
    } else {
      if(enableVerboseLogging) {
        LOG.warn("'{}' unknown service name!", serviceName);
      }

      return "0";
    }
  }

  private long hash(String partitionKey) {
    //A FNV-1 hash http://en.wikipedia.org/wiki/Fowler-Noll-Vo_hash_function
    long hash = 2166136261L;
    for (byte b:partitionKey.getBytes()) {
      hash=(hash * 16777619)^b;
    }
    return Math.abs(hash);
  }
} // class FilteryByShard