package module1.homework2;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
/**
 * Parses  log 
 * @author Oleh
 *
 */
 public final class LogRexExp{
	
	private static final String LOGENTRYPATTERN = "(ip\\d+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"";
    private static final Pattern PATTERN = Pattern.compile(LOGENTRYPATTERN);
    /**
     * Parses log line, throws IllegalArgumentException if finds bad log line
     * @param String  -> line
     * @return Log -> POJO holder for log properties
     * @throws IllegalArgumentException
     */
    public static Log parseApacheLog(String line) throws IllegalArgumentException{
    	Matcher matcher = PATTERN.matcher(line);
	    if (!matcher.matches()) {
	    	throw new IllegalArgumentException();
	    }
	    return new Log(matcher.group(1), matcher.group(4), matcher.group(5), matcher.group(6), matcher.group(7) , matcher.group(9)); 
    }
}
