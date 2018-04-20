package module1.homework1;

import static module1.homework1.StringUtils.breakLineToWords;
import static module1.homework1.StringUtils.stringBuilder;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.junit.Test;

public class StringUtilsTest {
	private String text = "What  human, contrivance could do that?";
	private final String WORDSEPARATOR = "\t";

	@Test
	public void breakLineToWordsTest() {
		String [] words = breakLineToWords(text);
		assertEquals(6, words.length);
	}
	@Test
	public void stringBuilderTest() {
		ArrayList<Text> list = new ArrayList<>();
		list.add(new Text("Human"));
		list.add(new Text("Could"));
		String string =stringBuilder(list, WORDSEPARATOR);
		assertEquals("Human\tCould\t", string);
		
	}
		

}
