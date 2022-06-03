package org.apache.avro;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class SchemaTest {

  private JsonNode schema;
  private Schema.Names names;
  private Schema expectedOutput;
  private boolean isExceptionExpected;

  public SchemaTest(ParamType jsonNodeType, ParamType namesType, boolean isExceptionExpected) {
    configure(jsonNodeType, namesType, isExceptionExpected);

  }

  private void configure(ParamType jsonNodeType, ParamType namesType, boolean isExceptionExpected) {

    this.isExceptionExpected = isExceptionExpected;
    try {
      String jsonStr;
      ObjectMapper mapper = new ObjectMapper();
      String nameStr = null;

      switch(namesType) {
        case NULL:
          this.names = null;
          break;
        case VALID:
          nameStr = "org.apache.avro";
          this.names = new Schema.Names(nameStr);
          break;
        case INVALID:
          this.names = new InvalidNames();
          break;
      }

      switch(jsonNodeType) {
        case PRIMITIVE:
          jsonStr = "{\"type\":\"string\"}";
          this.schema = mapper.readTree(jsonStr);
          this.expectedOutput = Schema.create(Schema.Type.STRING);
          break;

        case RECORD:
          jsonStr = "{\"type\":\"record\",\"name\":\"RecordName\",\"fields\":[{\"name\":\"Value\",\"type\":\"string\"}]}";
          this.schema = mapper.readTree(jsonStr);
          Schema nestedSchema  = Schema.create(Schema.Type.STRING);
          Schema.Field recordField = new Schema.Field("Value", nestedSchema, null, null);
          List<Schema.Field> recordFields = new ArrayList<>();
          recordFields.add(recordField);
          this.expectedOutput = Schema.createRecord("RecordName", null, nameStr, false, recordFields);
          break;

        case ENUM:
          jsonStr = "{\"type\":\"enum\",\"name\":\"EnumName\",\"symbols\":[\"COME\",\"QUANDO\",\"FUORI\",\"PIOVE\"]}";
          this.schema = mapper.readTree(jsonStr);
          List<String> enumValues = new ArrayList<>();
          enumValues.add("COME");
          enumValues.add("QUANDO");
          enumValues.add("FUORI");
          enumValues.add("PIOVE");
          this.expectedOutput = Schema.createEnum("EnumName", null, nameStr, enumValues);
          break;

        case ARRAY:
          jsonStr = "{\"type\":\"array\",\"items\":\"string\"}";
          this.schema = mapper.readTree(jsonStr);
          Schema elementType = Schema.create(Schema.Type.STRING);
          this.expectedOutput = Schema.createArray(elementType);
          break;

        case MAP:
          jsonStr = "{\"type\":\"map\",\"values\":\"string\"}";
          this.schema = mapper.readTree(jsonStr);
          Schema valueType = Schema.create(Schema.Type.STRING);
          this.expectedOutput = Schema.createMap(valueType);
          break;

        case UNION:
          jsonStr = "[\"null\",\"string\"]";
          this.schema = mapper.readTree(jsonStr);
          Schema firstType = Schema.create(Schema.Type.NULL);
          Schema secondType = Schema.create(Schema.Type.STRING);
          List<Schema> schemas = new ArrayList<>();
          schemas.add(firstType);
          schemas.add(secondType);
          this.expectedOutput = Schema.createUnion(schemas);
          break;

        case FIXED:
          jsonStr = "{\"type\":\"fixed\",\"size\":16,\"name\":\"md5\"}";
          this.schema = mapper.readTree(jsonStr);
          this.expectedOutput = Schema.createFixed("md5", null, nameStr, 16);
          break;

        case INVALID:
          jsonStr = "{\"type\":\"record\"}";
          this.schema = mapper.readTree(jsonStr);
          this.expectedOutput = null;
          break;

        case NULL:
          this.schema = null;
          this.expectedOutput = null;
          break;
      }

    } catch(Exception e) {
      Assert.fail("No exception should be thrown during configuration. Instead, " + e.getClass().getName() +
        " has been thrown.");
    }

  }

  @Parameterized.Parameters
  public static Collection<Object[]> getParameters() {
    return Arrays.asList(new Object[][] {
      //SCHEMA                NAMES                 EXCEPTION
      {ParamType.PRIMITIVE,   ParamType.NULL,       true},
      {ParamType.PRIMITIVE,   ParamType.VALID,      false},
      {ParamType.PRIMITIVE,   ParamType.INVALID,    true},
      {ParamType.RECORD,      ParamType.NULL,       true},
      {ParamType.RECORD,      ParamType.VALID,      false},
      {ParamType.RECORD,      ParamType.INVALID,    true},
      {ParamType.ENUM,        ParamType.NULL,       true},
      {ParamType.ENUM,        ParamType.VALID,      false},
      {ParamType.ENUM,        ParamType.INVALID,    true},
      {ParamType.ARRAY,       ParamType.NULL,       true},
      {ParamType.ARRAY,       ParamType.VALID,      false},
      {ParamType.ARRAY,       ParamType.INVALID,    true},
      {ParamType.MAP,         ParamType.NULL,       true},
      {ParamType.MAP,         ParamType.VALID,      false},
      {ParamType.MAP,         ParamType.INVALID,    true},
      {ParamType.UNION,       ParamType.NULL,       true},
      {ParamType.UNION,       ParamType.VALID,      false},
      {ParamType.UNION,       ParamType.INVALID,    false},
      {ParamType.FIXED,       ParamType.NULL,       true},
      {ParamType.FIXED,       ParamType.VALID,      false},
      {ParamType.FIXED,       ParamType.INVALID,    true},
      {ParamType.INVALID,     ParamType.NULL,       true},
      {ParamType.INVALID,     ParamType.VALID,      true},
      {ParamType.INVALID,     ParamType.INVALID,    true},
      {ParamType.NULL,        ParamType.NULL,       true},
      {ParamType.NULL,        ParamType.VALID,      true},
      {ParamType.NULL,        ParamType.INVALID,    true}
    });
  }

  @Test
  public void testParse() {

    try {
      Schema actualOutput = Schema.parse(this.schema, this.names);
      Assert.assertEquals("actualOutput != expectedOutput", this.expectedOutput, actualOutput);
      Assert.assertFalse("An exception was expected.", this.isExceptionExpected);

    } catch(Exception e) {
      Assert.assertTrue("No exception was expected, but " + e.getClass().getName() + " has been thrown.",
        this.isExceptionExpected);
    }

  }

  public enum ParamType {
    NULL, VALID, INVALID, PRIMITIVE, RECORD, ENUM, ARRAY, MAP, UNION, FIXED
  }

}
