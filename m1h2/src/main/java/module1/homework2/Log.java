package module1.homework2;

/**
 * Log properties holder POJO
 * 
 * @author Oleh
 *
 */
class Log {
	private String idAddress;
	private String dateTime;
	private String request;
	private String response;
	private String bytesSent;
	private String browser;

	public Log(String idAddress, String dateTime, String request, String response, String bytesSent,
			String browser) {
		super();
		this.idAddress = idAddress;
		this.dateTime = dateTime;
		this.request = request;
		this.response = response;
		this.bytesSent = bytesSent;
		this.browser = browser;
	}

	public String getIdAddress() {
		return idAddress;
	}

	public String getDateTime() {
		return dateTime;
	}

	public String getRequest() {
		return request;
	}

	public String getResponse() {
		return response;
	}

	public String getBytesSent() {
		return bytesSent;
	}

	public String getBrowser() {
		return browser;
	}

}
