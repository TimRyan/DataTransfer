package mine;

import java.sql.*;
import java.util.*;

/**
 * Data transfer class
 * 
 * @author Tim
 * 
 */
public class DataTransfer {

	private DBConn srcConn = null;
	private DBConn tarConn = null;
	private Connection srcOriginConn = null;
	private Connection tarOriginConn = null;

	/**
	 * Constructor of a data transfer
	 * 
	 * @param sourceConn
	 * @param targetConn
	 */
	public DataTransfer(DBConn sourceConn, DBConn targetConn) {

		srcConn = sourceConn;
		tarConn = targetConn;
		srcOriginConn = srcConn.getOriginConn();
		tarOriginConn = tarConn.getOriginConn();

	}

	/**
	 * 
	 * @param targetConn
	 */
	public DataTransfer(DBConn targetConn) {

		tarConn = targetConn;
		tarOriginConn = tarConn.getOriginConn();

	}

	/**
	 * 
	 * @param sourceTab
	 *            source table name including schema name
	 * @param targetTab
	 *            target table name including schema name
	 * @param batchSize
	 *            how many rows inserted in one transaction
	 */
	public void tableToTable(String sourceTab, String targetTab, int batchSize) {

		String insertSQL = null;
		String selectSQl = "select * from " + sourceTab; //+ " fetch first 10 rows only";
		ResultSet srcRS = srcConn.selectSQL(selectSQl);

		int batchCounter = 0;
		int columnsCount = 0;

		PreparedStatement psInsert = null;
		PreparedStatement psSelect = null;
		ResultSetMetaData rsMeta = null;

		// Do bulk insert
		try {
			psSelect = srcOriginConn.prepareStatement(selectSQl);
			rsMeta = psSelect.getMetaData();

			// Get columns count
			columnsCount = rsMeta.getColumnCount();
			// Assemble insert SQL
			insertSQL = "insert into " + targetTab + " values(?";
			for (int i = 1; i < columnsCount; i++) {
				insertSQL += ",?";
			}
			insertSQL += ")";

			psInsert = tarOriginConn.prepareStatement(insertSQL);

			System.out.println(insertSQL); // Test!!!!!!!!!!!!!!!!!!!!!!

			while (srcRS.next()) {

				for (int i = 1; i <= columnsCount; i++) {
					psInsert.setString(i, srcRS.getString(i));
				}

				psInsert.addBatch();

				if (++batchCounter == batchSize) {
					psInsert.executeBatch();
				}
			}
			psInsert.executeBatch();
			psInsert.close();
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}

	}

	/**
	 * 
	 * @param srcRS
	 *            ResultSet needs to be inserted
	 * @param targetTab
	 *            Target table name including schema name
	 * @param targetColList
	 *            Column names list in target table which has one to one
	 *            correspondence with ResultSet columns
	 * @param batchSize
	 *            how many rows inserted in one transaction
	 */
	public void columnsToColumns(ResultSet srcRS, String targetTab,
			ArrayList<String> targetColList, int batchSize){

		PreparedStatement psInsert = null;
		String insertSQL = null;
		int batchCounter = 0;

		// Do bulk insert
		try {

			// Assemble insert SQL
			insertSQL = "insert into " + targetTab + "(";
			int listSize = targetColList.size();
			String[] arrStrings = (String[]) targetColList
					.toArray(new String[listSize]);
			int columnsCount = arrStrings.length;

			for (int i = 0; i < columnsCount; i++) {
				if (i == columnsCount - 1) {
					insertSQL += arrStrings[i];
				} else {
					insertSQL += arrStrings[i] + ",";
				}
			}
			insertSQL += ") values(";
			for (int i = 0; i < columnsCount; i++) {
				if (i == columnsCount - 1) {
					insertSQL += "?";
				} else {
					insertSQL += "?,";
				}
			}
			insertSQL += ")";

			psInsert = tarOriginConn.prepareStatement(insertSQL);

			System.out.println(insertSQL); // Test!!!!!!!!!!!!!!!!!!!!!!

			while (srcRS.next()) {

				for (int i = 1; i <= columnsCount; i++) {
					psInsert.setString(i, srcRS.getString(i));
				}

				psInsert.addBatch();

				if (++batchCounter == batchSize) {
					psInsert.executeBatch();
				}
			}
	
			psInsert.executeBatch();
			
			psInsert.close();
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}
}
