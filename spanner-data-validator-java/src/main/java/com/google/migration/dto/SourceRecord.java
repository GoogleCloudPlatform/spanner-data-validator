package com.google.migration.dto;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;

@DefaultCoder(AvroCoder.class)
public class SourceRecord {

  private final List<SourceField> sourceFields;

  public SourceRecord() {
    this.sourceFields = new ArrayList<>();
  }

  public void addField(String fieldName, String fieldDataType, Object fieldValue) {
    this.sourceFields.add(new SourceField(fieldName, fieldDataType, fieldValue));
  }

  public SourceField getField(String fieldName) {
    return sourceFields.stream().filter(s -> s.getFieldName().equals(fieldName)).findFirst().orElse(null);
  }

  public SourceField getField(int index) {
    return sourceFields.get(index);
  }

  public int length() {
    return sourceFields.size();
  }

  @Override
  public String toString() {
    return "SourceRecord{" +
        "sourceFields=" + sourceFields +
        '}';
  }
}
