package util_sql;

import java.io.*;
import java.sql.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;

import org.w3c.dom.CDATASection;

import dataPreparation.createPostDataDatabase;


public class sqlUtils {

	public static String getQueryResultOneValue(Connection con, String in_query) throws Exception{
		Statement st=con.createStatement();
		ResultSet rs=st.executeQuery(in_query);
		rs.first();

		String res=rs.getString(1);

		rs.close();
		st.close();

		return res;
	}

	public static void writeQueryResultToFile(Connection con, String in_queryString, String in_fieldSeparatorForPrint, String in_nullFieldStringForPrint, boolean in_writeHeader, String out_file) throws Exception{
		//write result of given sql query string to given file using given separator between columns
		//uses line by line access to database so that full table will not be loaded into java heap space
		//WARNING: removes field-separator and newline chars from field values

		BufferedWriter writer=new BufferedWriter(new FileWriter(out_file));

		Statement st=con.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
		st.setFetchSize(Integer.MIN_VALUE);

		ResultSet rs;

		rs=st.executeQuery(in_queryString);
		ResultSetMetaData rsmd = rs.getMetaData();
		int numCols = rsmd.getColumnCount();
		if(in_writeHeader){
			//print header
			//System.out.print(rsmd.getColumnName(1));
			writer.append(rsmd.getColumnName(1));
			for(int c=2; c<=numCols; c++){
				//System.out.print(in_separator + rsmd.getColumnName(c));
				writer.append(in_fieldSeparatorForPrint + rsmd.getColumnName(c));
			}
			//System.out.println();
			writer.append("\n");
		}
		//print data
		while(rs.next()){
			String cValue=rs.getString(1);
			//System.out.println("|#|" + cValue + "|#|");
			if(cValue==null){
				cValue=in_nullFieldStringForPrint;
			}else{
				if(cValue.isEmpty()){
					//cValue=in_nullFieldStringForPrint;
					cValue=" ";
				}
			}
			cValue=cValue.replace(in_fieldSeparatorForPrint, " ");
			cValue=cValue.replace("\r\n", " ");
			cValue=cValue.replace("\r", " ");
			cValue=cValue.replace("\n", " ");
			cValue=cValue.replace("null", " ");
			cValue=cValue.replace("NULL", " ");
			
			writer.append(cValue);
			for(int c=2; c<=numCols; c++){
				cValue=rs.getString(c);
				//System.out.println("|#|" + cValue + "|#|"); //for debug
				if(cValue==null){
					cValue=in_nullFieldStringForPrint;
				}else{
					if(cValue.isEmpty()){
						//cValue=in_nullFieldStringForPrint;
						cValue="";
					}
				}
				cValue=cValue.replace(in_fieldSeparatorForPrint, " ");
				cValue=cValue.replace("\r\n", " ");
				cValue=cValue.replace("\r", " ");
				cValue=cValue.replace("\n", " ");
				cValue=cValue.replace("null", " ");
				cValue=cValue.replace("NULL", " ");
				writer.append(in_fieldSeparatorForPrint + cValue);
			}
			//System.out.println();
			writer.append("\n");
		}
		writer.flush();
		writer.close();
		
		st.close();
	}

	public static void printQueryResult(Connection con, String in_queryString, String in_separatorForPrint, String in_nullFieldStringForPrint) throws SQLException{
		//print result of given sql query string to stdout using given separator between columns and given string to be used for null

		Statement st=con.createStatement();
		ResultSet rs=st.getResultSet();

		rs=st.executeQuery(in_queryString);
		ResultSetMetaData rsmd = rs.getMetaData();
		int numCols = rsmd.getColumnCount();
		//print header
		System.out.print(rsmd.getColumnLabel(1));
		for(int c=2; c<=numCols; c++){
			System.out.print(in_separatorForPrint + rsmd.getColumnLabel(c));
		}
		System.out.println();
		//print data
		while(rs.next()){
			String cValue=rs.getString(1);
			if(cValue==null){
				cValue=in_nullFieldStringForPrint;
			}else{
				if(cValue.isEmpty()){
					cValue=in_nullFieldStringForPrint;
				}
			}
			System.out.print(cValue);
			for(int c=2; c<=numCols; c++){
				cValue=rs.getString(c);
				if(cValue==null){
					cValue=in_nullFieldStringForPrint;
				}else{
					if(cValue.isEmpty()){
						cValue=in_nullFieldStringForPrint;
					}
				}
				System.out.print(in_separatorForPrint + cValue);
			}
			System.out.println();
		}
	}


	
	
	public static void backupDatabase(Connection con, String in_sourceDb, String in_targetDb) throws SQLException{
		Statement st=con.createStatement();
		ResultSet rs=st.getResultSet();
		Statement stUp=con.createStatement();

		//create new db
		stUp.executeUpdate("create database " + in_targetDb + ";");

		//get list of tables in source db
		rs=st.executeQuery("show tables in " + in_sourceDb + ";");
		//loop through table names
		while(rs.next()){
			String currentTable=rs.getString(1);
			System.out.println("copying " + currentTable + " ...");
			//create this table in target db
			stUp.executeUpdate("create table " + in_targetDb + "." + currentTable + " like " + in_sourceDb + "." + currentTable + ";");
			//copy values
			stUp.executeUpdate("insert into " + in_targetDb + "." + currentTable + " select * from " + in_sourceDb + "." + currentTable + ";");
			con.commit();
		}
		
		con.commit();
		rs.close();
		st.close();
		stUp.close();

	}

	public static void backupTable( Connection con, String tableName, String newTableName ) throws SQLException {
		//backup table into new table with given name
		Statement stmt = con.createStatement();
		stmt.executeUpdate("drop table if exists " + newTableName + ";");
		stmt.executeUpdate("create table " + newTableName + " like " + tableName + ";");
		stmt.executeUpdate("insert into " + newTableName + " select * from " + tableName + ";");
		con.commit();
		stmt.close();
		
	}
	
	public static void readDataFileIntoTable(Connection con, String sourceFile, String fieldSeparator, String targetTable, LinkedHashMap<Integer, String> dataFieldTableFieldMapping, boolean insertIgnore) throws Exception{
		//expects field mapping hashmap in format: <fieldIndexInFile, tableFieldName[SPACE]fieldType> where fieldType must one of: "string", "int", "double", case insensitive
		
		LinkedHashMap<Integer, Integer> dataFieldPstFieldMapping=new LinkedHashMap<Integer, Integer>();
		HashMap<Integer, String> dataFieldType=new HashMap<Integer, String>();
		//create pst string and get data field to pst field mappings
		String insertStringFieldsPart="";
		String insertStringValuesPart="";
		int pstField=1;
		for(int cDataField:dataFieldTableFieldMapping.keySet()){
			String cTableField=dataFieldTableFieldMapping.get(cDataField).substring(0, dataFieldTableFieldMapping.get(cDataField).indexOf(" "));
			String cFieldType=dataFieldTableFieldMapping.get(cDataField).substring(dataFieldTableFieldMapping.get(cDataField).indexOf(" ")+1);
			insertStringFieldsPart=insertStringFieldsPart + cTableField + ",";
			insertStringValuesPart=insertStringValuesPart + "?,";
			dataFieldPstFieldMapping.put(cDataField, pstField);
			dataFieldType.put(cDataField, cFieldType);
			pstField++;
		}
		//remove trailing commas
		insertStringFieldsPart=insertStringFieldsPart.substring(0, insertStringFieldsPart.length()-1);
		insertStringValuesPart=insertStringValuesPart.substring(0, insertStringValuesPart.length()-1);
		//create pst string
		String insertString;
		if(insertIgnore){
			insertString="insert ignore into " + targetTable + " (" + insertStringFieldsPart + ") values (" + insertStringValuesPart + ")";
		}
		else{
			insertString="insert into " + targetTable + " (" + insertStringFieldsPart + ") values (" + insertStringValuesPart + ")";
		}
		
		//prepare statement
		PreparedStatement pst=con.prepareStatement(insertString);
		
		BufferedReader reader=new BufferedReader(new FileReader(sourceFile));
		
		//read file rows
		String line=null;
		while((line=reader.readLine()) != null){
			String[] lineArray=line.split(fieldSeparator);
			//loop through source fields
			for(int cField=0; cField<=lineArray.length; cField++){
				if(dataFieldPstFieldMapping.containsKey(cField)){
					int cPstField=dataFieldPstFieldMapping.get(cField);
					String cFieldType=dataFieldType.get(cField);
					if(cFieldType.equalsIgnoreCase("int")){
						pst.setInt(cPstField, Integer.parseInt(lineArray[cField]));
					}
					else if(cFieldType.equalsIgnoreCase("double")){
						pst.setDouble(cPstField, Double.parseDouble(lineArray[cField]));
					}
					else if(cFieldType.equalsIgnoreCase("string")){
						pst.setString(cPstField, lineArray[cField]);
					}
					else{
						System.out.println("ERROR: readDataFileIntoTable: error in field mappings type, type must be one of \"string\", \"int\", \"double\", but received: " + cFieldType);
						throw new Exception("readDataFileIntoTable: error in field mappings type, type must be one of \"string\", \"int\", \"double\", but received: " + cFieldType);
					}
					
				}
			}
			//exec pst
			pst.executeUpdate();
		}
		con.commit();
		pst.close();
	}


	public static void backupDatabaseAskUser(Connection con, String in_sourceDb, String in_targetDb) throws Exception{
		Statement st=con.createStatement();
		ResultSet rs=st.getResultSet();
		Statement stUp=con.createStatement();

		//create new db
		stUp.executeUpdate("create database " + in_targetDb + ";");

		//get list of tables in source db
		rs=st.executeQuery("show tables in " + in_sourceDb + ";");

		BufferedReader reader; //specify the reader variable //to be a standard input buffer 
		reader = new BufferedReader(new InputStreamReader(System.in));
		String confirm="n";

		//loop through table names
		while(rs.next()){
			String currentTable=rs.getString(1);
			//ask user 
			System.out.println("copy table " + currentTable + "? (y/n)");
			//read user response 
			confirm = reader.readLine(); 
			if(confirm.equalsIgnoreCase("y") || confirm.equalsIgnoreCase("yes")){
				//create this table in target db
				stUp.executeUpdate("create table " + in_targetDb + "." + currentTable + " like " + in_sourceDb + "." + currentTable + ";");
				//copy values
				stUp.executeUpdate("insert into " + in_targetDb + "." + currentTable + " select * from " + in_sourceDb + "." + currentTable + ";");
				con.commit();
			}
		}

		con.commit();
		rs.close();
		st.close();
		stUp.close();

		reader.close();

	}
	
	public static ResultSetMetaData getTableRsMetaData(Connection con, String in_sourceTable) throws Exception{
		Statement st=con.createStatement();
		ResultSet rs=st.executeQuery("select * from " + in_sourceTable + " limit 1");
		ResultSetMetaData rsMeta=rs.getMetaData();
		return rsMeta;
	}
	
	public static LinkedHashSet<String> getListOfTablesInDb(Connection con, String dbName, String filterLikeSqlRegexOptional) throws Exception{
		LinkedHashSet<String> res=new LinkedHashSet<String>();
		
		Statement st=con.createStatement();
		st.execute("use " + dbName);
		
		String query="show tables";
		if(filterLikeSqlRegexOptional!=null){
			query=query + " like " + filterLikeSqlRegexOptional;
		}
		
		ResultSet rs=st.executeQuery(query);
		while(rs.next()){
			res.add(rs.getString(1));
		}
		rs.close();
		st.close();
		return res;
	}

}