package module2.homework3;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import eu.bitwalker.useragentutils.UserAgent;
@Description(name = "user_agent_udf",
value = "function (string line) - input from UserAgent"+
        "returns structure with device, osname, browser, ua",
extended = "Example: select function(UserAgent string) from source;")
public class UserAgentUDF extends GenericUDF{

	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		List<String> fields = new ArrayList<String>();
        List<ObjectInspector> fieldObjectInspectors = new ArrayList<ObjectInspector>();
        fields.add("device");
        fieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        fields.add("osname");
        fieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        fields.add("browser");
        fieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        fields.add("ua");
        fieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        StructObjectInspector si = ObjectInspectorFactory.getStandardStructObjectInspector(fields, fieldObjectInspectors);
        return si;
	}

	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		if (arguments == null || arguments.length < 1||arguments[0].get() == null) {
            throw new HiveException("incorrect arguments");
        }
        Object arg0 = arguments[0].get();
        return parseUserAgent(arg0.toString());
	}

	public String getDisplayString(String[] children) {
		return "UDF which can parse any user agent (UA) string into separate fields";
	}
	private Object parseUserAgent(String argument) {
		Object[] userAgent = new Object[4];
        UserAgent ua = new UserAgent(argument);
        userAgent[0] = new Text(ua.getOperatingSystem().getDeviceType().getName());
        userAgent[1] = new Text(ua.getOperatingSystem().getName());
        userAgent[2] = new Text(ua.getBrowser().getGroup().getName());
        userAgent[3] = new Text(ua.getBrowser().getBrowserType().getName());
        return userAgent;
    }
	
	

}
