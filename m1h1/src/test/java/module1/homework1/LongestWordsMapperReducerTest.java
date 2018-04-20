package module1.homework1;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class LongestWordsMapperReducerTest {
	MapDriver<LongWritable, Text, CustomKey, Text> mapDriver;
	ReduceDriver<CustomKey, Text, CustomKey, Text> reduceDriver;
	MapReduceDriver<LongWritable, Text, CustomKey, Text, CustomKey, Text> mapReduceDriver;

	@Before
	public void setUp() throws Exception {
		LongestWordsMapper mapper = new LongestWordsMapper();
		mapDriver = MapDriver.newMapDriver(mapper);
		LongestWordsReducer reducer = new LongestWordsReducer();
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void longestWordsMapperTest() throws IOException {
		mapDriver.withInput(new LongWritable(), new Text("Hi"));
		mapDriver.withOutput(new CustomKey(2), new Text("Hi"));
		mapDriver.runTest();
	}

	@Test
	public void longestWordsReducerTest() throws IOException {
		ArrayList<Text> values = new ArrayList<>();
		values.add(new Text("Hi"));
		values.add(new Text("Hello"));
		reduceDriver.withInput(new CustomKey(3), values);
		reduceDriver.withOutput(new CustomKey(3), new Text("Hi\tHello\t"));
		reduceDriver.runTest();
	}

	@Test
	public void mapReduceTest() throws IOException {
		mapReduceDriver.withInput(new LongWritable(), new Text("Hi, Hello"));
		mapReduceDriver.addOutput(new CustomKey(5), new Text("Hello\t"));
		mapReduceDriver.addOutput(new CustomKey(2), new Text("Hi\t"));
		mapReduceDriver.runTest();
	}

}
