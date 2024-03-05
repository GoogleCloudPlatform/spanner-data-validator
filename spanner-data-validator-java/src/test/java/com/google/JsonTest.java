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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import org.json.JSONArray;
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

  @Test
  public void jsonArraySortTest() {
    ObjectMapper om = new ObjectMapper();
    try {
      JSONObject jo1 = new JSONObject()
          .put("JSON1", "ZHello World!")
          .put("JSON5", "Hello my World!")
          .put("JSON2", "Hello my World!")
          .put("JSON3", new JSONObject().put("key1", "value1")
              .put("key4", "value4")
              .put("key2", "value2"));

      JSONArray inner = new JSONArray();
      inner.put(new JSONObject().put("key324", "val342")
          .put("keyfdskfj", "valsdfkjs"));
      JSONObject joArray = new JSONObject()
          .put("arrayVal", inner);

      JSONObject jo2 = new JSONObject()
          .put("JSON3", "AHello World!")
          .put("JSON6", "EHello my World!")
          .put("JSON2", "DHello my World!")
          .put("JSON3", new JSONObject().put("key1", "value1")
              .put("key7", "value4")
              .put("key8", "value2"));

      JSONArray ja = new JSONArray();
      ja.put(jo2);
      ja.put(joArray);
      ja.put(jo1);

      String rawJsonInput = ja.toString();

      SortedSet<String> sortedStrings = new TreeSet<>();
      JSONArray jsonArray = new JSONArray(rawJsonInput);

      traverseJSONArray(jsonArray, sortedStrings);
      String compressedVal = compressSortedStrings(sortedStrings);

      System.out.println(compressedVal);
    } catch (Exception ex) {
      System.out.println(ex);
    }
  }

  private void traverseJSONObject(JSONObject jo, SortedSet<String> sortedStrings) {
    Iterator<String> keys = jo.keys();
    while(keys.hasNext()) {
      String key = keys.next();
      Object valObj = jo.get(key);
      if(valObj instanceof JSONObject) {
        traverseJSONObject(jo.getJSONObject(key), sortedStrings);
      } else if(valObj instanceof JSONArray) {
        traverseJSONArray(jo.getJSONArray(key), sortedStrings);
      } else {
        if(jo.isNull(key)) {
          sortedStrings.add(key);
        } else {
          sortedStrings.add(key + jo.getString(key));
        } // if/else
      } // if/else
    } // while
  }

  private void traverseJSONArray(JSONArray jsonArray, SortedSet<String> sortedStrings) {
    for(int i = 0; i < jsonArray.length(); i++) {
      Object obj = jsonArray.get(i);
      if(obj instanceof JSONObject) {
        traverseJSONObject(jsonArray.getJSONObject(i), sortedStrings);
      } else if(obj instanceof JSONArray) {
        traverseJSONArray(jsonArray.getJSONArray(i), sortedStrings);
      } else {
        throw new RuntimeException("SDKFJSDFKJF");
      }
    } // for
  }

  private String compressSortedStrings(SortedSet<String> sortedStrings) {
    StringBuilder sbVals = new StringBuilder();
    for(String val: sortedStrings) {
      sbVals.append(val);
    } // for

    return sbVals.toString();
  }
} // class