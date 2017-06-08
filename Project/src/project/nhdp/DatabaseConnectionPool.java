package project.nhdp;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.tomcat.jdbc.pool.DataSource;
import org.apache.tomcat.jdbc.pool.PoolProperties;

final public class DatabaseConnectionPool {
	// JDBC driver name and database URL
		private static final String JDBC_DRIVER = "org.sqlite.JDBC";
		private static final String DB_URL = "jdbc:sqlite:test.db";
		private static Connection connection = null;
		
		
		
		private DatabaseConnectionPool() {
			
		}
		
		public static Connection getDatabaseConnection() throws SQLException {
			if (connection == null) {
				PoolProperties p = new PoolProperties();
				p.setUrl(DB_URL);
				p.setDriverClassName(JDBC_DRIVER);
				p.setValidationQuery("SELECT 1");
				p.setMaxActive(1000);
				p.setInitialSize(100);
				DataSource datasource = new DataSource();
				datasource.setPoolProperties(p);
				connection = datasource.getConnection();
			}
			return connection;
		}
		
}
