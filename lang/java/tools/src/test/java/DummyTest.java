import org.junit.Test;

import org.apache.avro.tool.IdlTool;

import static junit.framework.TestCase.assertEquals;

public class DummyTest {

	@Test
	public void getNameTest() {

		IdlTool idlTool = new IdlTool();
		assertEquals("idl", idlTool.getName());
		assertEquals("Generates a JSON schema from an Avro IDL file", idlTool.getShortDescription());

	}

}
