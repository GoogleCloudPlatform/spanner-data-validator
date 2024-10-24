package com.google;

import com.google.migration.Helpers;
import com.google.migration.TableSpecList;
import com.google.migration.dto.ShardSpecJsonDef;
import com.google.migration.dto.TableSpec;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import org.junit.Test;

public class GCSTest {
  @Test
  public void getGcsFileTest() throws Exception {
    String result = Helpers.getFileFromGCS("kt-shared-project", "bigdata-stuff", "mysql/cmds.txt");
    System.out.println(result);
  }

  @Test
  public void tableSpecFromGCSTest() throws Exception {
    List<TableSpec> fromJsonFile = TableSpecList.getFromJsonFile("kt-shared-project",
        "gs://bigdata-stuff/svt/member-events-test-spec.json");
    System.out.println(fromJsonFile.get(0).getTableName());
  }

  @Test
  public void shardSpecFromGCSTest() throws Exception {
    ShardSpecJsonDef shardSpecJsonDef = ShardSpecJsonDef.fromJsonFile("kt-shared-project",
        "gs://bigdata-stuff/svt/shard-spec-sample-v1.json");
    System.out.println(shardSpecJsonDef.getHostCount());
  }

  @Test
  public void breakupGcsFileComponentsTest() throws URISyntaxException {
    String file = "gs://bigdata-stuff/mysql/cmds.txt";

    try {
      URI uri = new URI(file);
      System.out.println("Schem: " + uri.getScheme());
      System.out.println("Bucket: " + uri.getHost());
      System.out.println("Object: " + uri.getPath());
    } catch (URISyntaxException ex) {
      System.out.println("Not uri");
    }
  }
}