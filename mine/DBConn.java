package mine;

import java.sql.*;
import java.util.*;

/**
 * Database connection class
 * 
 * @author Tim
 * 
 */
public class DBConn {

	private Connection conn = null;
	private String urlString = null;
	private String driverString = null;

	/**
	 * Constructor of a connection. Connection URL Example:
	 * "jdbc:db2://192.168.56.210:60000/SAMPLE"
	 * 
	 * @param dbType
	 * @param host
	 * @param port
	 * @param dbName
	 * @param user
	 * @param password
	 */
	public DBConn(String dbType, String host, String port, String dbName,
			String user, String password) {

		// Make sure connection information is complete
		if (dbType == null || host == null || port == null || dbName == null
				|| user == null || password == null) {
			System.out.println("Connection information not complete");
			return;
		}

		// Deal with different database connection string
		switch (dbType) {
		case "DB2":
			driverString = "com.ibm.db2.jcc.DB2Driver";
			urlString = "jdbc:db2://" + host + ":" + port + "/" + dbName;
			break;
		default:
			System.out.println("Invalid database type");
			return;
		}

		// Construct a connection
		try {
			Class.forName(driverString).newInstance();
			conn = DriverManager.getConnection(urlString, user, password);
		} catch (Exception e) {
			System.out.print(e.getMessage());
		}
	}

	/**
	 * Run 'SELECT' SQL in here in order to get a result set Return a ResultSet
	 * 
	 * @param sql
	 * @return Query result set
	 */
	public ResultSet selectSQL(String sql) {
		ResultSet rs = null;
		try {
			PreparedStatement ps = conn.prepareStatement(sql);
			System.out.printf("Executed SQL:\n%s\n", sql);
			rs = ps.executeQuery();
		} catch (SQLException e) {
			System.out.print(e.getMessage());
		}

		return rs;
	}

	/**
	 * Run SQL without any result returned
	 * 
	 * @param sql
	 * @return 'true' for successful run and 'false' for failed run
	 */
	public Boolean execSQl(String sql) {
		Boolean result = true;

		try {
			PreparedStatement ps = conn.prepareStatement(sql);
			System.out.printf("Executed SQL:\n%s\n", sql);
			result = ps.execute();
		} catch (SQLException e) {
			result = false;
			System.out.print(e.getMessage());
		}

		return result;
	}

	/**
	 * Get columns name of a table
	 * 
	 * @param schemaName
	 * @param tabName
	 * @param type
	 * @return ArrayList<String> contains column names or null
	 */
	public ArrayList<String> getColumnNames(String schemaName, String tabName,
			String type) {

		ArrayList<String> colNameList = new ArrayList<String>();

		String sql = null;
		ResultSet rsColNames = null;

		if (schemaName == null || tabName == null) {
			System.out
					.println("parameter schemaName or tabName is null in method 'public ArrayList<String> getColumnNames'");
			return colNameList;
		}

		switch (type) {
		case "PRIMARY":
			sql = "select COLNAME from SYSCAT.KEYCOLUSE where TABSCHEMA = "
					+ "'" + schemaName.toUpperCase() + "'" + " and "
					+ "TABNAME =" + "'" + tabName.toUpperCase() + "'";
			break;

		default:
			sql = "select COLNAME from syscat.columns where TABSCHEMA = " + "'"
					+ schemaName.toUpperCase() + "'" + " and " + "TABNAME ="
					+ "'" + tabName.toUpperCase() + "'";
			break;
		}

		rsColNames = selectSQL(sql);

		try {
			while (rsColNames.next()) {
				colNameList.add(rsColNames.getString("COLNAME"));
			}
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}

		return colNameList;
	}

	/**
	 * Getting the original connection object generated using JDBC
	 * 
	 * @return conn
	 */
	public Connection getOriginConn() {
		return conn;
	}
	
	

	/**
	 * Close connection and result set
	 */
	public void close() {
		try {
			if (conn != null) {
				conn.close();
			}

		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		

	}

}
