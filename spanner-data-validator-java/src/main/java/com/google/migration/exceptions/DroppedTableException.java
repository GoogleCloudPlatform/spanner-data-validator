package com.google.migration.exceptions;

/** Exceptions thrown during Datastream Change Event conversions. */
public class DroppedTableException extends Exception {
  public DroppedTableException(Exception e) {
    super(e);
  }

  public DroppedTableException(String message) {
    super(message);
  }

  public DroppedTableException(String message, Exception e) {
    super(message, e);
  }
}

