package org.apache.avro.reflect;

import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Arrays;

import static org.mockito.Mockito.mock;

@RunWith(Parameterized.class)
public class ReflectDataSchemaIT {

  private Type type;
  private Map<String, Schema> names;
  private Schema expectedOutput;
  private boolean isExceptionExpected;
  private boolean isExceptionThrown = false;

  public ReflectDataSchemaIT(ParamType typeType, NamesType namesType, boolean isExceptionExpected) {
    configure(typeType, namesType, isExceptionExpected);

  }

  private void configure(ParamType typeType, NamesType namesType, boolean isExceptionExpected) {

    this.isExceptionExpected = isExceptionExpected;
    try {
      switch (namesType) {
      case NULL:
        this.names = null;
        break;
      case EMPTY:
        this.names = new HashMap<>();
        break;
      case VAL_VAL:
        this.names = new HashMap<>();
        Schema schema1 = Schema.create(Schema.Type.STRING);
        Schema schema2 = Schema.create(Schema.Type.NULL);
        this.names.put("Schema1", schema1);
        this.names.put("Schema2", schema2);
        break;
      case VAL_INV:
        this.names = new HashMap<>();
        Schema schema = Schema.createEnum(null, null, null, null);
        this.names.put("Schema", schema);
        break;
      case VAL_MOCK:
        this.names = new HashMap<>();
        Schema mockedSchema = getMockedSchema();
        this.names.put("Mocked_Schema", mockedSchema);
        break;
      case VAL_NULL:
        this.names = new HashMap<>();
        this.names.put("No_Schema", null);
        break;
      case EMP_VAL:
        this.names = new HashMap<>();
        Schema valSchema = Schema.create(Schema.Type.BOOLEAN);
        this.names.put("", valSchema);
        break;
      case EMP_INV:
        this.names = new HashMap<>();
        Schema invSchema = Schema.createRecord(null, null, null, false, null);
        this.names.put("", invSchema);
        break;
      case EMP_MOCK:
        this.names = new HashMap<>();
        Schema mockedSchema2 = getMockedSchema();
        this.names.put("", mockedSchema2);
        break;
      case EMP_NULL:
        this.names = new HashMap<>();
        this.names.put("", null);
        break;
      case NULL_VAL:
        this.names = new HashMap<>();
        Schema valSchema2 = Schema.create(Schema.Type.FLOAT);
        this.names.put(null, valSchema2);
        break;
      case NULL_INV:
        this.names = new HashMap<>();
        Schema invSchema2 = Schema.createEnum(null, null, null, null);
        this.names.put(null, invSchema2);
        break;
      case NULL_MOCK:
        this.names = new HashMap<>();
        Schema mockedSchema3 = getMockedSchema();
        this.names.put(null, mockedSchema3);
        break;
      case NULL_NULL:
        this.names = new HashMap<>();
        this.names.put(null, null);
        break;
      }

      switch (typeType) {
      case NULL:
        this.type = null;
        this.expectedOutput = null;
        break;
      case INVALID:
        Schema[] arraySch = null;
        this.type = arraySch.getClass().getComponentType();
        this.expectedOutput = null;
        break;
      case STRING:
        String[] arrayStr = {};
        this.type = arrayStr.getClass().getComponentType();
        this.expectedOutput = Schema.create(Schema.Type.STRING);
        break;
      case ERROR:
        Exception[] arrayExcep = {};
        this.type = arrayExcep.getClass().getComponentType();
        this.expectedOutput = Schema.createRecord("Exception", null, "java.lang", true);
        // Now we are creating the fields of an exception
        List<Schema> schemas = new ArrayList<>();
        Schema firstType = Schema.create(Schema.Type.NULL);
        Schema secondType = Schema.create(Schema.Type.STRING);
        schemas.add(firstType);
        schemas.add(secondType);
        Schema nestedSchema = Schema.createUnion(schemas);
        List<Schema.Field> fields = new ArrayList<>();
        Schema.Field field = new Schema.Field("detailMessage", nestedSchema, null, null);
        fields.add(field);
        this.expectedOutput.setFields(fields);
        break;
      case VOID:
        this.type = Void.TYPE;
        this.expectedOutput = Schema.create(Schema.Type.NULL);
        break;
      case BOOLEAN:
        boolean[] arrayBool = {};
        this.type = arrayBool.getClass().getComponentType();
        this.expectedOutput = Schema.create(Schema.Type.BOOLEAN);
        break;
      case INT_ARR:
        int[] arrayInt = {};
        this.type = arrayInt.getClass();
        Schema elementType2 = Schema.create(Schema.Type.INT);
        this.expectedOutput = Schema.createArray(elementType2);
        this.expectedOutput.addProp("java-class", "[I");
        break;
      case BYTE:
        this.type = Byte.TYPE;
        this.expectedOutput = Schema.create(Schema.Type.INT);
        this.expectedOutput.addProp("java-class", "java.lang.Byte");
        break;
      case SHORT:
        short[] arrayShort = {};
        this.type = arrayShort.getClass().getComponentType();
        this.expectedOutput = Schema.create(Schema.Type.INT);
        this.expectedOutput.addProp("java-class", "java.lang.Short");
        break;
      case CHAR:
        char[] arrayChar = {};
        this.type = arrayChar.getClass().getComponentType();
        this.expectedOutput = Schema.create(Schema.Type.INT);
        this.expectedOutput.addProp("java-class", "java.lang.Character");
        break;
      }

    } catch (NullPointerException e1) {
      Assert.assertTrue(
          "NullPointerException should be thrown during configuration only if Type is invalid or"
              + "Schema is invalid. Instead, " + e1.getClass().getName() + " has been thrown and namesType == "
              + namesType + ".",
          namesType == NamesType.VAL_INV || namesType == NamesType.EMP_INV || namesType == NamesType.NULL_INV
              || typeType == ParamType.INVALID);
      this.isExceptionThrown = true;

    } catch (Exception e2) {
      Assert.fail("No exception different from NullPointerException should be thrown during configuration. Instead, "
          + e2.getClass().getName() + " has been thrown.");
    }

  }

  @Parameterized.Parameters
  public static Collection<Object[]> getParameters() {
    return Arrays.asList(new Object[][] {
        // TYPE NAMES EXCEPTION
        { ParamType.NULL, NamesType.NULL, true }, { ParamType.NULL, NamesType.EMP_MOCK, true },
        { ParamType.NULL, NamesType.EMP_NULL, true }, { ParamType.INVALID, NamesType.EMP_VAL, true },
        { ParamType.INVALID, NamesType.EMP_INV, true }, { ParamType.STRING, NamesType.EMPTY, false },
        { ParamType.STRING, NamesType.VAL_VAL, false }, { ParamType.STRING, NamesType.VAL_INV, true },
        { ParamType.ERROR, NamesType.VAL_MOCK, false }, { ParamType.VOID, NamesType.VAL_NULL, false },
        { ParamType.BOOLEAN, NamesType.NULL_VAL, false }, { ParamType.INT_ARR, NamesType.NULL_INV, true },
        { ParamType.INT_ARR, NamesType.NULL_MOCK, false }, { ParamType.INT_ARR, NamesType.NULL_NULL, false },
        { ParamType.BYTE, NamesType.VAL_VAL, false }, { ParamType.SHORT, NamesType.VAL_VAL, false },
        { ParamType.CHAR, NamesType.VAL_VAL, false } });
  }

  private Schema getMockedSchema() {

    return mock(Schema.class);

  }

  @Test
  public void itCreateSchema() {

    try {
      if (this.isExceptionThrown) {
        Assert.assertTrue(
            "No exception was expected, but an exception during configuration phase has" + " been thrown.",
            this.isExceptionExpected);
      } else {
        ReflectData rd = new ReflectData();
        Schema actualOutput = rd.createSchema(this.type, this.names);
        Assert.assertEquals("actualOutput != expectedOutput", this.expectedOutput, actualOutput);
        Assert.assertFalse("An exception was expected.", this.isExceptionExpected);
      }

    } catch (Exception e) {
      Assert.assertTrue("No exception was expected, but " + e.getClass().getName() + " has been thrown.",
          this.isExceptionExpected);
    }

  }

  private enum ParamType {
    NULL, INVALID, STRING, ERROR, VOID, BOOLEAN, INT_ARR, BYTE, SHORT, CHAR
  }

  private enum NamesType {
    NULL, EMPTY, VAL_VAL, VAL_INV, VAL_MOCK, VAL_NULL, EMP_VAL, EMP_INV, EMP_MOCK, EMP_NULL, NULL_VAL, NULL_INV,
    NULL_MOCK, NULL_NULL
  }

}
