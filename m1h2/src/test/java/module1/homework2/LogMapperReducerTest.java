	package module1.homework2;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class LogMapperReducerTest {
	MapDriver<LongWritable, Text, Text, CountAverageTuple> mapDriver;
	ReduceDriver<Text, CountAverageTuple, Text, CountAverageTuple> reduceDriver;
	MapReduceDriver<LongWritable, Text,Text,CountAverageTuple, Text, CountAverageTuple> mapReduceDriver;
	String logLine;
	Log apacheLog;
	Text ipAdress;
	CountAverageTuple countAverageTuple;

	@Before
	public void setUp() throws Exception {
		LogMapper mapper = new LogMapper();
		mapDriver = MapDriver.newMapDriver(mapper);
		LogReducer reducer = new LogReducer();
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
		logLine="ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 40028 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\"";
		apacheLog=LogRexExp.parseApacheLog(logLine);
		ipAdress=new Text(apacheLog.getIdAddress());
		countAverageTuple =new CountAverageTuple(1f,Float.parseFloat(apacheLog.getBytesSent()));
	}

	@Test
	public void logMapperTest() throws IOException {
		mapDriver.withInput(new LongWritable(), new Text(logLine));
		mapDriver.withOutput(ipAdress,countAverageTuple);
		mapDriver.runTest();
	}

	@Test
	public void logReducerTest() throws IOException {
		List<CountAverageTuple> values = Arrays.asList(countAverageTuple,countAverageTuple);
		reduceDriver.withInput(ipAdress, values);
		float sum = 0;
		float count = 0;
		for (CountAverageTuple val : values) {
			sum += val.getCount() * val.getAverage();
			count += val.getCount();

		}
		countAverageTuple.setCount(sum);
		countAverageTuple.setAverage(sum/count);
		reduceDriver.withOutput(ipAdress, countAverageTuple);
		reduceDriver.runTest();
	}
	@Test
	public void mapReduceTest() throws IOException {
		mapReduceDriver.withInput(new LongWritable(), new Text(logLine));
		countAverageTuple.setCount(Float.parseFloat(apacheLog.getBytesSent()));
		mapReduceDriver.addOutput(ipAdress, countAverageTuple);
		mapReduceDriver.runTest();
	}


	
}
