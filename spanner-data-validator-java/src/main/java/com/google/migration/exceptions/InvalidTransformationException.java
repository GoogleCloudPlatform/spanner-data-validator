package com.google.migration.exceptions;

public class InvalidTransformationException extends Exception {
  public InvalidTransformationException(Exception e) {
    super(e);
  }

  public InvalidTransformationException(String message) {
    super(message);
  }

  public InvalidTransformationException(String message, Exception e) {
    super(message, e);
  }
}
