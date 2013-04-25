/** Class: IdGenerator
 * Summary: Generates ids for given values from given ids table.
 * If new values are encountered new ids are generated for them and written to ids table automatically.
 * WARNING: Has to be closed with a call to close(), otherwise PreparedStatement pst will remain open.
 * @author Nikolas Landia
 */ 


package util_sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;

public class IdGenerator {
	
	//< value, id >
	public HashMap<String, Integer> existingIds=new HashMap<String, Integer>();
	
	boolean newIdsAdded=false;
	int maxId=0;
	
	Connection con;
	String tableName;
	
	int idField=1;
	int valueField=2;
	
	PreparedStatement pst; //prepared statement for inserting new ids into database
	
	public IdGenerator(Connection in_con, String in_idTableName) throws Exception{
		con=in_con;
		tableName=in_idTableName;
		readIdsTable();
		pst=con.prepareStatement("insert into " + tableName + " values (?,?);");
	}
	
	public IdGenerator(Connection in_con, String in_idTableName, String in_idField, String in_valueField) throws Exception{
		con=in_con;
		tableName=in_idTableName;
		readIdsTable();
		pst=con.prepareStatement("insert into " + tableName + " (" + in_idField + ", " + in_valueField + ") values (?,?);");
	}
	
	public void readIdsTable() throws Exception{
		Statement st=con.createStatement();
		ResultSet rs=st.executeQuery("select * from " + tableName + ";");
		
		while(rs.next()){
			int cId=rs.getInt(idField);
			String cValue=rs.getString(valueField);
			existingIds.put(cValue.toLowerCase(), cId);
			if(cId>maxId){
				maxId=cId;
			}
		}
		rs.close();
		st.close();
	}
	
	public int getId(String in_value) throws Exception{
		String value=in_value.toLowerCase();
		if(existingIds.containsKey(value)){
			//return existing id
			return existingIds.get(value);
		}
		else{
			//generate new id
			int newId=++maxId;
			
			//add to database table
			pst.setInt(1, newId);
			pst.setString(2, value);
			pst.execute();
			//con.commit(); //dont commit here, its too slow
			
			//add to existing ids hashmap
			existingIds.put(value, newId);
			
			//return new id
			return newId;
		}
	}
	
	public void commit() throws Exception{
		con.commit();
	} 
	
	public void close() throws Exception{
		con.commit();
		pst.close();
	}
}
