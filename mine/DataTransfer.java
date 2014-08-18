package mine;

import java.sql.*;

/**
 * Data transfer class
 * 
 * @author Tim
 * 
 */
public class DataTransfer {

	DBConn srcConn = null;
	DBConn tarConn = null;

	/**
	 * Constructor of a data transfer
	 * 
	 * @param sourceConn
	 * @param targetConn
	 */
	public DataTransfer(DBConn sourceConn, DBConn targetConn) {

		srcConn = sourceConn;
		tarConn = targetConn;
	}

	/**
	 * 
	 * @param sourceTab
	 * @param targetTab
	 */
	public void tableToTable(String sourceTab, String targetTab, int batchSize) {

		String insertSQL = null;
		String selectSQl = "select * from " + sourceTab + " fetch first 10000 rows only";
		ResultSet srcRS = srcConn.selectSQL(selectSQl);
		Connection srcOriginConn = srcConn.getOriginConn();
		Connection tarOriginConn = tarConn.getOriginConn();

		int count = 0;
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

				if (++count == batchSize) {
					psInsert.executeBatch();
				}
			}
			psInsert.executeBatch();
			psInsert.close();
			srcOriginConn.close();
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}

	}
}
