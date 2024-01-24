package com.google;

import static org.junit.Assert.assertEquals;

import com.google.migration.TableSpecList;
import com.google.migration.dto.TableSpec;
import java.util.List;
import org.junit.Test;

public class TableSpecTest {
  @Test
  public void tableSpecReadFromJsonTest() throws Exception {
    List<TableSpec> tsList =
        TableSpecList.getFromJsonFile("json/sample-tablespec.json");

    assertEquals(tsList.size(), 1);
  }
}