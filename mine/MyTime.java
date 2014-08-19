package mine;

import java.sql.Date;
import java.text.SimpleDateFormat;

/**
 * Time class
 * 
 * @author Tim
 * 
 */
public class MyTime {

	/**
	 * Getting current date and time
	 * 
	 * @return current date+time in format "yyyy-MM-dd hh:mm:ss"
	 */
	public static String nowTime() {
		Date nowTime = new Date(System.currentTimeMillis());
		SimpleDateFormat sdFormatter = new SimpleDateFormat(
				"yyyy-MM-dd hh:mm:ss");
		String retStrFormatNowDate = sdFormatter.format(nowTime);
		return retStrFormatNowDate;
	}
}
