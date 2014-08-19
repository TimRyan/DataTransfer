package mine;

import java.util.ArrayList;

/**
 * Construct specified data structure with arguments
 * 
 * @author Tim
 * 
 */
public class Maker {
	/**
	 * 
	 * @param args
	 * @return
	 */
	public static ArrayList<String> makeArrayList(String[] args) {

		ArrayList<String> result = new ArrayList<String>();
		for (int i = 0; i < args.length; i++) {
			result.add(args[i]);
		}
		return result;
	}
}
