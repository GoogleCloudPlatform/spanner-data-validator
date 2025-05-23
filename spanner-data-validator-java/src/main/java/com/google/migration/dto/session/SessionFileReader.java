package com.google.migration.dto.session;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class to read the HarbourBridge session file in GCS and convert it into a Schema object. */
public class SessionFileReader {
  private static final Logger LOG = LoggerFactory.getLogger(SessionFileReader.class);

  public static Schema read(String sessionFilePath) {
    if (sessionFilePath == null) {
      return new Schema();
    }
    return readFileIntoMemory(sessionFilePath);
  }

  private static void validateSessionFields(JsonObject sessionJSON) {
    if (!sessionJSON.has("SpSchema")) {
      throw new IllegalArgumentException("Cannot find \"SpSchema\" field in session file.");
    }
    if (!sessionJSON.has("SrcSchema")) {
      throw new IllegalArgumentException("Cannot find \"SrcSchema\" field in session file.");
    }
    if (!sessionJSON.has("SyntheticPKeys")) {
      throw new IllegalArgumentException("Cannot find \"SyntheticPKeys\" field in session file.");
    }
  }

  private static Schema readFileIntoMemory(String filePath) {
    try (InputStream stream =
        Channels.newInputStream(FileSystems.open(FileSystems.matchNewResource(filePath, false)))) {
      String result = IOUtils.toString(stream, StandardCharsets.UTF_8);
      JsonParser parser = new JsonParser();
      JsonObject sessionJSON = parser.parseString(result).getAsJsonObject();
      validateSessionFields(sessionJSON);

      Schema schema =
          new GsonBuilder()
              .setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE)
              .create()
              .fromJson(result, Schema.class);
      schema.setEmpty(false);
      schema.generateMappings();
      LOG.debug("Read session file: " + schema.toString());
      return schema;
    } catch (IOException e) {
      LOG.error(
          "Failed to read session file. Make sure it is ASCII or UTF-8 encoded and contains a"
              + " well-formed JSON string.",
          e);
      throw new RuntimeException(
          "Failed to read session file. Make sure it is ASCII or UTF-8 encoded and contains a"
              + " well-formed JSON string.",
          e);
    }
  }
}
