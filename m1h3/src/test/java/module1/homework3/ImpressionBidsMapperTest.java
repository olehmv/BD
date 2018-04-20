package module1.homework3;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class ImpressionBidsMapperTest {
	ImpressionBidsMapper mapper;
	MapDriver<LongWritable, Text, CustomKey, LongWritable> driver;

	@Before
	public void setUp() throws Exception {
		mapper = new ImpressionBidsMapper();
		driver=MapDriver.newMapDriver(mapper);
	}

	@Test
	public void impressionBidsMapperTest() throws IOException {
		driver.withAll(readInput());
		driver.withCacheFile(getCacheFile().toURI());
		driver.addAllOutput(readOutput());
		driver.runTest();
	}
	public  File getCacheFile() {
		ClassLoader classLoader = getClass().getClassLoader();
		File file = new File(classLoader.getResource("cache/city.en.txt").getFile());
		return file;
	}
	public  List<Pair<LongWritable, Text>> readInput() {
		List<Pair<LongWritable, Text>> input = new ArrayList<>();
		@SuppressWarnings("resource")
		Scanner scanner = new Scanner(ImpressionBidsMapperTest.class.getResourceAsStream("/input/m_input.txt"))
				.useDelimiter("\n");
		while (scanner.hasNext()) {
			input.add(new Pair<LongWritable, Text>(new LongWritable(), new Text(scanner.next().trim())));
		}
		scanner.close();
		return input;
	}

	public  List<Pair<CustomKey, LongWritable>> readOutput() {
		List<Pair<CustomKey, LongWritable>> output = new ArrayList<>();
		@SuppressWarnings("resource")
		Scanner scanner = new Scanner(ImpressionBidsMapper.class.getResourceAsStream("/output/m_out.txt"))
				.useDelimiter("\n");
		while (scanner.hasNext()) {
			String[] out = scanner.next().trim().split("\t");
			output.add(new Pair<CustomKey, LongWritable>(new CustomKey(out[0].trim(),out[1].trim()), new LongWritable(Long.valueOf(out[2].trim()))));
		}
		scanner.close();
		return output;
	}

}


