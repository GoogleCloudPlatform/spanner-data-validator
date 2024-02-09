package com.google;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.TreeMap;
import org.json.JSONObject;
import org.junit.Test;

public class JsonTest {
  @Test
  public void jsonKeySortTest() {
    ObjectMapper om = new ObjectMapper();
    try {
      String rawJsonInput = new JSONObject()
          .put("JSON1", "Hello World!")
          .put("JSON5", "Hello my World!")
          .put("JSON2", "Hello my World!")
          .put("JSON3", new JSONObject().put("key1", "value1")
                                        .put("key4", "value4")
                                        .put("key2", "value2"))
          .toString();

      // https://stackoverflow.com/questions/30325042/how-to-compare-two-json-strings-when-the-order-of-entries-keep-changing
      TreeMap<String, Object> m1 = (TreeMap<String, Object>) (om.readValue(rawJsonInput, TreeMap.class));

      System.out.println(om.writeValueAsString(m1));
    } catch (Exception ex) {
    }
  }
} // class