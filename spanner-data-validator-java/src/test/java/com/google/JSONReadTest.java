package com.google;

import static org.junit.Assert.assertEquals;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import org.junit.Test;

public class JSONReadTest {
  public class SimpleTestDTO {

    public String getTest() {
      return test;
    }

    public void setTest(String test) {
      this.test = test;
    }

    private String test;
  }
  @Test
  public void simpleReadTest() throws Exception {
    ClassLoader classloader = Thread.currentThread().getContextClassLoader();
    InputStream is = classloader.getResourceAsStream("json/simple-test.json");
    Reader reader = new InputStreamReader(is);

    Gson gson = new Gson();
    SimpleTestDTO dto = new Gson().fromJson(reader, SimpleTestDTO.class);
    assertEquals("value", dto.getTest());
  }
} // class JSONReadTest