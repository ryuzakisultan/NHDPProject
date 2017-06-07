package project.nhdp;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NHDPJavaProject {
	// JDBC driver name and database URL
	static final String JDBC_DRIVER = "org.sqlite.JDBC";
	static final String DB_URL = "jdbc:sqlite:test.db";

	static final String CSV_URL = "C:/Users/sahmed07/Downloads/JDBC Training/Project/data.csv";
	static final int BATCH_SIZE = 100;

	private static final Logger log = LogManager.getLogger(NHDPJavaProject.class.getName());

	public static void main(String[] args) {
		log.trace("Trace Message");
		log.debug("Debug Message");
		log.info("Info Message");
		log.warn("Warn Message");
		log.error("Error Message");
		log.fatal("Fatal Message");
		
		
		// Initialize a blocking queue for buffering data
		//TODO LOG
		BlockingQueue<String> queue = new LinkedBlockingQueue<String>();

		// Read CSV file data into Blocking Queue
		//TODO LOG
		try (Scanner s = new Scanner(new File(CSV_URL))) {
			while (s.hasNext()) {
				//TODO LOG
				queue.add(s.nextLine());
			}
			s.close();
		} catch (FileNotFoundException ex) {
			//TODO LOG
			ex.printStackTrace();
		}

		// Register JDBC Driver
		try {
			//TODO LOG
			Class.forName(JDBC_DRIVER);
		} catch (ClassNotFoundException ex) {
			ex.printStackTrace();
			//TODO LOG
		}

		// Table creation
		String creationSQL = "CREATE TABLE TRANS_REQUESTS " + "(trace_audit_no INT PRIMARY KEY     NOT NULL,"
				+ " card_prg_id		VARCHAR(50)    NOT NULL, " + " trans_date date	DATE     NOT NULL, "
				+ " service_id        VARCHAR(50) NOT NULL)";

		// String sql = "select * from trans_requests";
		String insertSQL = "INSERT INTO TRANS_REQUESTS" + "(trace_audit_no, card_prg_id, trans_date, service_id) VALUES"
				+ "(?,?,?,?)";

		String sql = "delete from trans_requests";
		Connection con = null;
		PreparedStatement ps = null;
		Statement stmt = null;
		DatabaseMetaData dbm = null;
		ResultSet tables = null;

		try {
			//TODO LOG
			con = DriverManager.getConnection(DB_URL);
			//TODO LOG
			// If table doesn't exist create table
			try {
				dbm = con.getMetaData();
				//TODO LOG
				tables = dbm.getTables(null, null, "TRANS_REQUESTS", null);
				if (!tables.next()) {
					//TODO LOG
					stmt = con.createStatement();
					//TODO LOG
					stmt.executeUpdate(creationSQL);
				}
			} catch (SQLException ex) {
				ex.printStackTrace();
			} finally {
				if (stmt != null) {
					//TODO LOG
					stmt.close();
				}
				if (tables != null) {
					//TODO LOG
					tables.close();
				}
			}
			
			ps = con.prepareStatement(insertSQL);
			//TODO

			con.setAutoCommit(false);
			//TODO LOG
			int count = 0;
			int batchesProcessed = 0;
			//TODO LOG
			for (String row : queue) {
				String[] fields = row.split(",");
				ps.setInt(1, Integer.parseInt(fields[0]));
				ps.setString(2, fields[1]);
				ps.setDate(3, Date.valueOf(fields[2]));
				ps.setString(4, fields[3]);
				ps.addBatch();
				if (++count % BATCH_SIZE == 0) {
					ps.executeBatch();
					batchesProcessed++;
					//TODO LOG
				}
			}
			ps.executeBatch();
			//TODO LOG
			con.setAutoCommit(true);
			//TODO LOG
			
			// int count = stmt.executeUpdate(sql);
			// System.out.println(count);
			//
		} catch (SQLException ex) {
			//TODO LOG
			ex.printStackTrace();
		} finally {
			if (ps != null) {
				try {
					ps.close();
					//TODO LOG
				} catch (SQLException e) {
					//TODO LOG
					e.printStackTrace();
				}
			}
			if (con != null) {
				try {
					con.close();
					//TODO LOG
				} catch (SQLException e) {
					//TODO LOG
					e.printStackTrace();
				}
			}
		}

	}
}
