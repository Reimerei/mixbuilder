package util_sql;

import java.sql.*;

public class ConnectionMaker {
	static Connection conMySql = null;
	static Connection conMsSql = null;
	static Connection conSqLite = null;

	static Connection[] conMySqlMulti = null;

	public static Connection[] getConnectionMySqlMulti(String in_conStr, int in_numConnections) throws SQLException {
		//if array already contains connections, close them
		if ( conMySqlMulti != null ) {
			for(int c=0; c<conMySqlMulti.length; c++){
				conMySqlMulti[c].close();
			}
			conMySqlMulti=null;
		}
		try {
			Class.forName("com.mysql.jdbc.Driver");
			for(int c=0; c<in_numConnections; c++){
				conMySqlMulti[c] = DriverManager.getConnection( in_conStr );
				conMySqlMulti[c].setAutoCommit( false );
			}
		} catch ( ClassNotFoundException e ) {
			System.err.println("Exception Occurs:\nMessage: " + e.getMessage());
			e.printStackTrace();
		} catch ( SQLException e ) {
			System.err.println("Exception Occurs:\nMessage: " + e.getMessage());
			e.printStackTrace();
			throw e;
		}
		return conMySqlMulti;
	}

	public static Connection getConnectionMySql(String in_conStr) throws SQLException {
		if ( conMySql == null || conMySql.isClosed()) {
			try {
				Class.forName("com.mysql.jdbc.Driver");
				conMySql = DriverManager.getConnection( in_conStr );
				conMySql.setAutoCommit( false );
			} catch ( ClassNotFoundException e ) {
				System.err.println("Exception Occurs:\nMessage: " + e.getMessage());
				e.printStackTrace();
			} catch ( SQLException e ) {
				System.err.println("Exception Occurs:\nMessage: " + e.getMessage());
				e.printStackTrace();
				throw e;
			}
		}

		return conMySql;
	}

	public static Connection getNewConnectionMySql(String in_conStr) throws SQLException {
		Connection con=null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			con = DriverManager.getConnection( in_conStr );
			con.setAutoCommit( false );
		} catch ( ClassNotFoundException e ) {
			System.err.println("Exception Occurs:\nMessage: " + e.getMessage());
			e.printStackTrace();
		} catch ( SQLException e ) {
			System.err.println("Exception Occurs:\nMessage: " + e.getMessage());
			e.printStackTrace();
			throw e;
		}

		return con;
	}

	//--------------------------------------------------------------------------------------------
	//MS-SQL

	public static Connection getConnectionMsSql(String in_conStr) throws SQLException {
		if ( conMsSql == null ) {
			try {
				Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
				conMsSql = DriverManager.getConnection(in_conStr);
			} catch ( ClassNotFoundException e ) {
				System.err.println("Exception Occurs:\nMessage: " + e.getMessage());
				e.printStackTrace();
			} catch ( SQLException e ) {
				System.err.println("Exception Occurs:\nMessage: " + e.getMessage());
				e.printStackTrace();
				throw e;
			}
		}
		return conMsSql;
	}

	//--------------------------------------------------------------------------------------------
	//SQL Lite

	public static Connection getConnectionSqLite(String in_conStr) throws SQLException {
		if ( conSqLite == null ) {
			try {
				Class.forName("org.sqlite.JDBC");
				conSqLite = DriverManager.getConnection(in_conStr);
			} catch ( ClassNotFoundException e ) {
				System.err.println("Exception Occurs:\nMessage: " + e.getMessage());
				e.printStackTrace();
			} catch ( SQLException e ) {
				System.err.println("Exception Occurs:\nMessage: " + e.getMessage());
				e.printStackTrace();
				throw e;
			}
		}
		return conSqLite;
	}
}