package module2.homework3;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.junit.Before;
import org.junit.Test;

public class UserAgentUDFTest {
	static UserAgentUDF uaUDF;
	static DeferredJavaObject defObj;
	static String log = "a80fc47c7b544c78eb54a793e72a198e	20131027170602323	1	CAV6UQBndoT	Mozilla/5.0 (Windows NT 5.1; rv:24.0) Gecko/20100101 Firefox/24.0	10.237.95.*	0	4	1	d2c2382e14b7d7de3a32218dba3192b1	503afcab3679835909f87eedbc559b0	null	mm_45472847_4168669_13664047	950	90	Na	Na	300	12628	294	287	null	2261	13866\n";

	@Before
	public void setUp() throws Exception {
		uaUDF = new UserAgentUDF();
		defObj = new DeferredJavaObject(log);
	}

	@Test
	public void testEvaluate() throws HiveException {
		Object[] evaluated = (Object[]) uaUDF.evaluate(new DeferredJavaObject[] { defObj });
		assertEquals(evaluated[0].toString(), "Computer");
		assertEquals(evaluated[1].toString(), "Windows XP");
		assertEquals(evaluated[2].toString(), "Firefox");
		assertEquals(evaluated[3].toString(), "Browser");
	}

	@Test(expected = HiveException.class)
	public void testException() throws HiveException {
		uaUDF.evaluate(new DeferredJavaObject[] {});

	}

}
