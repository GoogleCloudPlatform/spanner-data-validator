package com.google.migration.common;

import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JSONNormalizer {
  private static final Logger LOG = LoggerFactory.getLogger(JSONNormalizer.class);
  public static String getNormalizedString(String rawJsonInput) {
    SortedSet<String> sortedStrings = new TreeSet<>();

    try {
      JSONObject jo = new JSONObject(rawJsonInput);
      traverseJSONObject(jo, sortedStrings);
    } catch(Exception ex) {
      try {
        JSONArray ja = new JSONArray(rawJsonInput);
        traverseJSONArray(ja, sortedStrings);
      } catch(Exception exIn) {
        return rawJsonInput;
      } // try/catch
    } // try/catch

    String compressedVal = compressSortedStrings(sortedStrings);

    return compressedVal;
  }

  private static void traverseJSONObject(JSONObject jo, SortedSet<String> sortedStrings) {
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
          sortedStrings.add(key + jo.get(key));
        } // if/else
      } // if/else
    } // while
  }

  private static void traverseJSONArray(JSONArray jsonArray, SortedSet<String> sortedStrings) {
    for(int i = 0; i < jsonArray.length(); i++) {
      if(!jsonArray.isNull(i)) {
        Object obj = jsonArray.get(i);
        if (obj instanceof JSONObject) {
          traverseJSONObject(jsonArray.getJSONObject(i), sortedStrings);
        } else if (obj instanceof JSONArray) {
          traverseJSONArray(jsonArray.getJSONArray(i), sortedStrings);
        } else {
          LOG.debug(String.format("Not an object or array: %s", obj.toString()));
          sortedStrings.add(obj.toString());
        } // if/else
      } // if
    } // for
  }

  private static String compressSortedStrings(SortedSet<String> sortedStrings) {
    StringBuilder sbVals = new StringBuilder();
    for(String val: sortedStrings) {
      sbVals.append(val);
    } // for

    return sbVals.toString();
  }
} // class