package org.apache.avro;

public class InvalidNames extends Schema.Names {

  @Override
  public String space() {
    throw new NullPointerException();
  }

}
