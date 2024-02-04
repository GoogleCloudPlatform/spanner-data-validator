package com.google;

import static org.junit.Assert.assertEquals;

import com.google.migration.Helpers;
import java.math.BigInteger;
import java.util.UUID;
import org.junit.Test;

public class SecretTest {
  @Test
  public void getSecretTest() throws Exception {
    String result = Helpers.getSecret("kt-shared-project", "dbpass", "latest");
    System.out.println(result);
  }
} // class