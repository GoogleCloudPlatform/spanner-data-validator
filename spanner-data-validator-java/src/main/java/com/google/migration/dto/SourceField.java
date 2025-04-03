package com.google.migration.dto;

import java.io.Serializable;

public class SourceField implements Serializable {

  private final String fieldName;

  private final String fieldDataType;

  private final Object fieldValue;

  public SourceField(String fieldName, String fieldDataType, Object fieldValue) {
    this.fieldName = fieldName;
    this.fieldDataType = fieldDataType;
    this.fieldValue = fieldValue;
  }

  public String getFieldName() {
    return fieldName;
  }

  public String getFieldDataType() {
    return fieldDataType;
  }

  public Object getFieldValue() {
    return fieldValue;
  }

  @Override
  public String toString() {
    return "SourceField{" +
        "fieldName='" + fieldName + '\'' +
        ", fieldDataType='" + fieldDataType + '\'' +
        ", fieldValue=" + fieldValue +
        '}';
  }
}
