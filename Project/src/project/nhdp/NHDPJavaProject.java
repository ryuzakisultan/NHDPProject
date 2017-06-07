package project.nhdp;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NHDPJavaProject {
	// JDBC driver name and database URL
	static final String JDBC_DRIVER = "org.sqlite.JDBC";
	static final String DB_URL = "jdbc:sqlite:d:/sqlite/test.db";
	
	
	static final String CSV_URL = "C:/Users/sahmed07/Downloads/JDBC Training/Project/data.csv";
	static final int BATCH_SIZE = 100;
	
	static final Logger log = LogManager.getLogger(NHDPJavaProject.class);
	
	public static void main(String[] args) {
		//Initialize a blocking queue for buffering data
		BlockingQueue<String> queue = new LinkedBlockingQueue<String>();
		
		//Read CSV file data into Blocking Queue
		try (Scanner s = new Scanner(new File(CSV_URL))) {
			while (s.hasNext()) {
				queue.add(s.nextLine());
			}
			s.close(); 
		} catch (FileNotFoundException ex) {
			ex.printStackTrace();
		}
		
		//Register JDBC Driver
		try {
			Class.forName(JDBC_DRIVER);
		} catch (ClassNotFoundException ex) {
			ex.printStackTrace();
		}
		
		//Table creation
		/*String creationSQL = "CREATE TABLE TRANS_REQUESTS " +
		                        "(trace_audit_no INT PRIMARY KEY     NOT NULL," +
		                        " card_prg_id		VARCHAR(50)    NOT NULL, " + 
		                        " trans_date date	DATE     NOT NULL, " + 
		                        " service_id        VARCHAR(50) NOT NULL)";*/
		
		
		//String sql = "select * from trans_requests";
		String insertSQL = "INSERT INTO TRANS_REQUESTS" +
							"(trace_audit_no, card_prg_id, trans_date, service_id) VALUES" +
							"(?,?,?,?)";
		
		//String sql = "delete from trans_requests where trace_audit_no = 058"; 
		try (Connection con = DriverManager.getConnection(DB_URL);
				PreparedStatement ps = con.prepareStatement(insertSQL); 
				) {
			
			int count = 0;
			for (String row : queue) {
				String[] fields = row.split(",");
				ps.setInt(1, Integer.parseInt(fields[0]));
				ps.setString(2, fields[1]);
				ps.setDate(3, Date.valueOf(fields[2]));
				ps.setString(4, fields[3]);
				ps.addBatch();
				if (++count % BATCH_SIZE == 0) {
					ps.executeBatch();
				}
			}
			ps.executeBatch();
		} catch (SQLException ex) {
			ex.printStackTrace();
		}
		
		
		
	}
}
