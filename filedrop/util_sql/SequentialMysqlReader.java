package util_sql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class SequentialMysqlReader {
	
	Connection con;
	Statement st;
	ResultSet rs;

	public SequentialMysqlReader(String conStr, String query) throws Exception{
		con=ConnectionMaker.getNewConnectionMySql(conStr);
		st=con.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
		st.setFetchSize(Integer.MIN_VALUE);
		
		rs=st.executeQuery(query);
	}
	
	public ResultSet getResultSet(){
		return rs;
	}
	
	public void close() throws Exception{
		rs.close();
		st.close();
		con.close();
	}
}
