package project.nhdp;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;


public class NHDPJavaProject {
	static final String CSV_URL = "data.csv";
	static final int BATCH_SIZE = 100;

	private static final Logger log = LogManager.getLogger(NHDPJavaProject.class.getName());
	private static final Marker SQL_MARKER = MarkerManager.getMarker("SQL");
	
	public static void main(String[] args) {
		// Initialize a blocking queue for buffering data
		log.info("Initializing LinkedBlockingQueue buffer");
		BlockingQueue<String> queue = new LinkedBlockingQueue<String>();

		// Read CSV file data into Blocking Queue
		log.info("CSV file to Buffer reading started");
		try (Scanner s = new Scanner(new File(CSV_URL))) {
			while (s.hasNext()) {
				String line = s.nextLine();
				log.trace("Adding line \"{}\" to Buffer",line);
				queue.add(line);
			}
			s.close();
		} catch (FileNotFoundException ex) {
			log.error(ex,ex);
		}
/*
		// Register JDBC Driver
		try {
			log.info("Registring JDBC Driver");
			Class.forName(JDBC_DRIVER);
		} catch (ClassNotFoundException ex) {
			log.error(ex,ex);
		}
*/
		// Table creation
		String creationSQL = "CREATE TABLE TRANS_REQUESTS " + "(trace_audit_no INT PRIMARY KEY     NOT NULL,"
				+ " card_prg_id		VARCHAR(50)    NOT NULL, " + " trans_date date	DATE     NOT NULL, "
				+ " service_id        VARCHAR(50) NOT NULL)";

		// String sql = "select * from trans_requests";
		String insertSQL = "INSERT INTO TRANS_REQUESTS" + "(trace_audit_no, card_prg_id, trans_date, service_id) VALUES"
				+ "(?,?,?,?)";
		String formattedInsertSQL = "INSERT INTO TRANS_REQUESTS" + "(trace_audit_no, card_prg_id, trans_date, service_id) VALUES"
				+ "({},{},{},{})";

		Connection con = null;
		PreparedStatement ps = null;
		Statement stmt = null;
		DatabaseMetaData dbm = null;
		ResultSet tables = null;

		try {
			log.info("Getting Database connection via DriverManager");
			con = DatabaseConnectionPool.getDatabaseConnection();
			
			// If table doesn't exist create table
			try {
				log.info("Getting Database MetaData");
				dbm = con.getMetaData();
				log.info("Check if TRANS_REQUESTS table exists in database");
				tables = dbm.getTables(null, null, "TRANS_REQUESTS", null);
				if (!tables.next()) {
					log.info("Table doesn't exist in database");
					log.trace("Get a simple statmenet from connection");
					stmt = con.createStatement();
					log.trace("executing table creation query");
					log.info(SQL_MARKER,creationSQL);
					stmt.executeUpdate(creationSQL);
				}
			} catch (SQLException ex) {
				log.error(ex,ex);
			} finally {
				if (stmt != null) {
					log.trace("Closing statment");
					stmt.close();
					log.trace("Statement closed");
				}
				if (tables != null) {
					log.trace("closing tables ResultSet");
					tables.close();
					log.trace("tables ResultSet closed");
				}
			}
			
			log.trace("Creating prepared statement: " + insertSQL);
			ps = con.prepareStatement(insertSQL);
			log.trace("Prepared statement created");
			log.info("setting setAutoCommit to false");
			con.setAutoCommit(false);
			int count = 0;
			int batchesProcessed = 0;
			log.info("Batch processing starting");
			for (String row : queue) {
				String[] fields = row.split(",");
				ps.setInt(1, Integer.parseInt(fields[0]));
				ps.setString(2, fields[1]);
				ps.setDate(3, Date.valueOf(fields[2]));
				ps.setString(4, fields[3]);
				ps.addBatch();
				log.trace(SQL_MARKER,"Following SQL added to batch " + (batchesProcessed + 1) + ": " + formattedInsertSQL,fields[0], "'" + fields[1] + "'", "'" + fields[2] + "'", "'" + fields[3] + "'" );
				if (++count % BATCH_SIZE == 0) {
					log.info("executing batch " + (batchesProcessed +1));
					ps.executeBatch();
					batchesProcessed++;
					log.info(batchesProcessed + " batches executed successfully");
				}
			}
			log.info("Executing last batch");
			ps.executeBatch();
			
			log.info("Setting autoCommit to true");
			con.setAutoCommit(true);
			
		} catch (SQLException ex) {
			log.error(ex,ex);
		} finally {
			if (ps != null) {
				try {
					log.info("Closing prepared statment");
					ps.close();
					log.info("Prepared statement closed");
				} catch (SQLException e) {
					log.error(e,e);
				}
			}
			if (con != null) {
				try {
					log.info("Closing database connection");
					con.close();
					log.info("Database connection closed");
				} catch (SQLException e) {
					log.error(e,e);
				}
			}
		}

	}
}
