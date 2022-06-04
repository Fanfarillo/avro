package org.apache.avro.reflect;

import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@RunWith(Parameterized.class)
public class ReflectDataSchemaIT {

  private Type type;
  private Map<String, Schema> names;
  private Schema expectedOutput;
  private boolean isExceptionExpected;
  private boolean isExceptionThrown = false;

  public ReflectDataSchemaIT(ParamType typeType, ParamType namesType, boolean isExceptionExpected) {
    configure(typeType, namesType, isExceptionExpected);

  }

  private void configure(ParamType typeType, ParamType namesType, boolean isExceptionExpected) {

    this.isExceptionExpected = isExceptionExpected;
    try {
      switch (namesType) {
      case NULL:
        this.names = null;
        break;
      case EMPTY:
        this.names = new HashMap<>();
        break;
      case VALID:
        this.names = new HashMap<>();
        Schema schema1 = Schema.create(Schema.Type.STRING);
        Schema schema2 = Schema.create(Schema.Type.NULL);
        this.names.put("Schema1", schema1);
        this.names.put("Schema2", schema2);
        break;
      case INVALID:
        this.names = new HashMap<>();
        Schema schema = Schema.createEnum(null, null, null, null);
        this.names.put(null, schema);
        break;
      }

      switch (typeType) {
      case NULL:
        this.type = null;
        this.expectedOutput = null;
        break;
      case VALID:
        String[] arrayStr = {};
        this.type = arrayStr.getClass().getComponentType();
        this.expectedOutput = Schema.create(Schema.Type.STRING);
        break;
      case INVALID:
        ParamType[] arrayPT = {};
        this.type = arrayPT.getClass().getComponentType();
        this.expectedOutput = null;
        break;
      }

    } catch (NullPointerException e1) {
      Assert.assertSame(
          "NullPointerException should be thrown during configuration only if namesType == INVALID." + " Instead, "
              + e1.getClass().getName() + " has been thrown and namesType == " + namesType + ".",
          namesType, ParamType.INVALID);
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
        { ParamType.NULL, ParamType.NULL, true }, { ParamType.VALID, ParamType.EMPTY, false },
        { ParamType.VALID, ParamType.VALID, false }, { ParamType.INVALID, ParamType.INVALID, true } });
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

  public enum ParamType {
    NULL, EMPTY, VALID, INVALID
  }

}
