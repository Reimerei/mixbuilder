package util_sql;

import java.sql.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;

public class sqlTableIntoHashmap {

	public static HashMap<Integer, HashMap<Integer, Integer>> getIntContainerAndIntIntDistinctKeys(Connection con, String in_queryWithFieldsInOrder) throws Exception{

		Statement st= con.createStatement();
		ResultSet rs= st.executeQuery(in_queryWithFieldsInOrder);
		HashMap<Integer, HashMap<Integer, Integer>> res= new HashMap<Integer, HashMap<Integer,Integer>>();
		while(rs.next()){
			int containerId=rs.getInt(1);
			int itemOneId=rs.getInt(2);
			int itemTwoId=rs.getInt(3);
			
			if(res.containsKey(containerId)==false){
				res.put(containerId, new HashMap<Integer, Integer>());
			}
			if(res.get(containerId).containsKey(itemOneId)){
				throw new Exception("ERROR: duplicate itemOneId: " + itemOneId);
			}
			res.get(containerId).put(itemOneId, itemTwoId);
		}
		rs.close();
		st.close();

		return res;
	}
	
	public static HashMap<Integer, Integer> getIntIntDistinctKeys(Connection con, String in_query) throws Exception{
		HashMap<Integer, Integer> res= new HashMap<Integer, Integer>();

		Statement st= con.createStatement();
		ResultSet rs= st.executeQuery(in_query);
		while(rs.next()){
			int currentKey=rs.getInt(1);
			int currentItem= rs.getInt(2);
			if(res.containsKey(currentKey)){
				System.out.println("ERROR: getTwoIntColumnsDistinctPairs: duplicate entry for key: " + currentKey);
				return null;
			}
			else{
				res.put(currentKey, currentItem);
			}
		}
		rs.close();
		st.close();

		return res;
	}
	
	public static HashMap<Integer, Double> getIntDoubleDistinctKeys(Connection con, String in_query) throws Exception{
		HashMap<Integer, Double> res= new HashMap<Integer, Double>();

		Statement st= con.createStatement();
		ResultSet rs= st.executeQuery(in_query);
		while(rs.next()){
			int currentKey=rs.getInt(1);
			double currentVal= rs.getDouble(2);
			if(res.containsKey(currentKey)){
				System.out.println("ERROR: getTwoIntColumnsDistinctPairs: duplicate entry for key: " + currentKey);
				return null;
			}
			else{
				res.put(currentKey, currentVal);
			}
		}
		rs.close();
		st.close();

		return res;
	}
	
	public static HashMap<Integer, HashSet<Integer>> getIntAndIntListDistinctPairs(Connection con, String in_sourceTable, String in_containerIdField, String in_itemIdField) throws Exception{
		String sourceTable=in_sourceTable;
		String containerIdField=in_containerIdField;
		String itemIdField=in_itemIdField;

		Statement st= con.createStatement();
		ResultSet rs= st.executeQuery("select " + containerIdField + ", " + itemIdField + " from " + sourceTable);
		HashMap<Integer, HashSet<Integer>> res= new HashMap<Integer, HashSet<Integer>>();
		while(rs.next()){
			int currentId=rs.getInt(containerIdField);
			int currentWordId= rs.getInt(itemIdField);
			if(res.containsKey(currentId)){
				res.get(currentId).add(currentWordId);
			}
			else{
				res.put(currentId, new HashSet<Integer>());
				res.get(currentId).add(currentWordId);
			}
		}
		rs.close();
		st.close();

		return res;
	}

	public static HashMap<String, Integer> getStringIntDistinctKeys(Connection con, String in_query) throws Exception{
		HashMap<String, Integer> res= new HashMap<String, Integer>();	

		Statement st= con.createStatement();
		ResultSet rs= st.executeQuery(in_query);
		while(rs.next()){
			String currentKey=rs.getString(1);
			int currentItem= rs.getInt(2);
			if(res.containsKey(currentKey)){
				System.out.println("ERROR: getTwoIntColumnsDistinctPairs: duplicate entry for key: " + currentKey);
				return null;
			}
			else{
				res.put(currentKey, currentItem);
			}
		}
		rs.close();
		st.close();

		return res;
	}
	
	public static HashMap<String, Double> getStringDoubleDistinctKeys(Connection con, String in_query) throws Exception{
		HashMap<String, Double> res= new HashMap<String, Double>();	

		Statement st= con.createStatement();
		ResultSet rs= st.executeQuery(in_query);
		while(rs.next()){
			String currentKey=rs.getString(1);
			Double currentItem= rs.getDouble(2);
			if(res.containsKey(currentKey)){
				System.out.println("ERROR: getTwoIntColumnsDistinctPairs: duplicate entry for key: " + currentKey);
				return null;
			}
			else{
				res.put(currentKey, currentItem);
			}
		}
		rs.close();
		st.close();

		return res;
	}

	
	public static HashMap<Integer, String> getIntStringDistinctKeys(Connection con, String in_query) throws Exception{
		HashMap<Integer, String> res= new HashMap<Integer, String>();	

		Statement st= con.createStatement();
		ResultSet rs= st.executeQuery(in_query);
		while(rs.next()){
			int currentKey=rs.getInt(1);
			String currentItem= rs.getString(2);
			if(res.containsKey(currentKey)){
				System.out.println("ERROR: getTwoIntColumnsDistinctPairs: duplicate entry for key: " + currentKey);
				return null;
			}
			else{
				res.put(currentKey, currentItem);
			}
		}
		rs.close();
		st.close();

		return res;
	}
	
	
	public static HashSet<String> getStringListDistinct(Connection con, String in_query) throws SQLException{

		Statement st=con.createStatement();
		ResultSet rs=st.getResultSet();

		HashSet<String> results = new HashSet<String>();
		rs=st.executeQuery(in_query);
		
		while(rs.next()){
			results.add(rs.getString(1));
		}

		rs.close();
		st.close();

		return results;
	}
	
	public static LinkedHashSet<String> getStringListDistinctLinkedHashSet(Connection con, String in_query) throws SQLException{

		Statement st=con.createStatement();
		ResultSet rs=st.getResultSet();

		LinkedHashSet<String> results = new LinkedHashSet<String>();
		rs=st.executeQuery(in_query);
		
		while(rs.next()){
			results.add(rs.getString(1));
		}

		rs.close();
		st.close();

		return results;
	}
	
	public static HashSet<Integer> getIntListDistinct(Connection con, String in_query) throws SQLException{

		Statement st=con.createStatement();
		ResultSet rs=st.getResultSet();

		HashSet<Integer> results = new HashSet<Integer>();
		rs=st.executeQuery(in_query);
		
		while(rs.next()){
			results.add(rs.getInt(1));
		}

		rs.close();
		st.close();

		return results;
	}
	
@Deprecated //use new version which takes query string: getStringListDistinct
	public static HashSet<String> getOneColumnAsString(Connection con, String in_sourceTable, String in_columnName) throws SQLException{
		String sourceTable=in_sourceTable;

		Statement st=con.createStatement();
		ResultSet rs=st.getResultSet();

		HashSet<String> results = new HashSet<String>();
		rs=st.executeQuery("select distinct " + in_columnName + " from " + sourceTable);
		
		while(rs.next()){
			results.add(rs.getString(in_columnName));
		}

		rs.close();
		st.close();

		return results;
	}


	//######################################################################
	//OLD
	/*
	public static LinkedHashMap<Integer, String> getAnyTableRowsAsStrings(Connection con, String in_sourceTable, String columnSeperator, String endString) throws SQLException{
		String sourceTable=in_sourceTable;

		Statement st= con.createStatement();
		ResultSet rs= st.executeQuery("select * from " + sourceTable + endString + ";");
		ResultSetMetaData rsMeta= rs.getMetaData();
		int numColumns=rsMeta.getColumnCount();
		LinkedHashMap<Integer, String> result= new LinkedHashMap<Integer, String>();
		//loop through rows 
		int rowCounter=1;
		int progressCount=0;
		while(rs.next()){

			//print progress
			if(progressCount%1000000==0)
				System.out.println("");
			if(progressCount%100000==0)
				System.out.print(".");
			progressCount++;

			String currentEntry="";
			//loop through colums
			for(int currentColumn=1; currentColumn<=numColumns; currentColumn++){
				currentEntry=currentEntry+rs.getString(currentColumn) + columnSeperator;
			}
			result.put(rowCounter, currentEntry);
			rowCounter++;
		}

		return result;
	}



	

	public static LinkedHashSet<Integer> getOneColumnAnyTableIntOrdered(Connection con, String in_sourceTable, String in_columnName) throws SQLException{
		String sourceTable=in_sourceTable;

		Statement st=con.createStatement();
		ResultSet rs=st.getResultSet();

		LinkedHashSet<Integer> results = new LinkedHashSet<Integer>();

		rs=st.executeQuery("select distinct " + in_columnName + " from " + sourceTable + " order by " + in_columnName + ";");

		while(rs.next()){
			results.add(rs.getInt(in_columnName));
		}

		rs.close();
		st.close();

		return results;
	}

	public static HashMap<Integer, Integer> getOneColumnAnyTableIntWithDuplicateCount(Connection con, String in_sourceTable, String in_columnName) throws SQLException{
		String sourceTable=in_sourceTable;

		Statement st=con.createStatement();
		ResultSet rs=st.getResultSet();

		HashMap<Integer, Integer> results = new HashMap<Integer, Integer>();

		rs=st.executeQuery("select " + in_columnName + " from " + sourceTable + " order by " + in_columnName + ";");

		while(rs.next()){
			int val=rs.getInt(in_columnName);
			int count=1;
			if(results.containsKey(val)){
				count=results.get(val)+1;
			}
			results.put(val, count);
		}

		rs.close();
		st.close();

		return results;
	}

	public static HashSet<Integer> getOneColumnAnyTableInt(Connection con, String in_sourceTable, String in_columnName, boolean distinct) throws SQLException{
		String sourceTable=in_sourceTable;

		Statement st=con.createStatement();
		ResultSet rs=st.getResultSet();

		LinkedHashSet<Integer> results = new LinkedHashSet<Integer>();
		if(distinct){
			rs=st.executeQuery("select distinct " + in_columnName + " from " + sourceTable + ";");
		}else{
			rs=st.executeQuery("select " + in_columnName + " from " + sourceTable + ";");
		}


		while(rs.next()){
			results.add(rs.getInt(in_columnName));
		}

		rs.close();
		st.close();

		return results;
	}



	public static HashMap<Integer, String> getIntStringDistinct(Connection con, String in_sourceTable, String idsFieldName, String wordsFieldName) throws SQLException{
		String sourceTable=in_sourceTable;

		Statement st=con.createStatement();
		ResultSet rs=st.getResultSet();

		HashMap<Integer, String> results = new HashMap<Integer, String>();
		rs=st.executeQuery("select " + idsFieldName + ", " + wordsFieldName + " from " + sourceTable + ";");
		while(rs.next()){
			results.put(rs.getInt(idsFieldName), rs.getString(wordsFieldName));
		}

		rs.close();
		st.close();

		return results;
	}

	public static HashMap<Integer, Integer> getIntWordsDfTable(Connection con, String in_sourceTable) throws SQLException{
		String sourceTable=in_sourceTable;

		Statement st=con.createStatement();
		ResultSet rs=st.getResultSet();

		HashMap<Integer, Integer> results = new HashMap<Integer, Integer>();
		rs=st.executeQuery("select word_id, doc_occur from " + sourceTable + ";");
		while(rs.next()){
			results.put(rs.getInt("word_id"), rs.getInt("doc_occur"));
		}

		rs.close();
		st.close();

		return results;
	}


	public static HashMap<String, LinkedHashMap<String, Double>> getScoresTableStringStringDouble(Connection con, String in_query) throws SQLException{

		//put words_count_tfidf table into nested hashmap, linked hashmap, linked hashmap is ordered ordered by tfidf desc
		Statement stWordsCountTfidf= con.createStatement();
		ResultSet rsWordsCountTfidf= stWordsCountTfidf.executeQuery(in_query);
		HashMap<String, LinkedHashMap<String, Double>> wordsCountTfidf= new HashMap<String, LinkedHashMap<String,Double>>();
		while(rsWordsCountTfidf.next()){
			String currentDocId=rsWordsCountTfidf.getString(1);
			String currentWordId=rsWordsCountTfidf.getString(2);
			double currentTfidf=rsWordsCountTfidf.getDouble(3);
			if(wordsCountTfidf.containsKey(currentDocId)){
				wordsCountTfidf.get(currentDocId).put(currentWordId, currentTfidf);
			}
			else{
				wordsCountTfidf.put(currentDocId, new LinkedHashMap<String, Double>());
				wordsCountTfidf.get(currentDocId).put(currentWordId, currentTfidf);
			}
		}
		rsWordsCountTfidf.close();
		stWordsCountTfidf.close();

		return wordsCountTfidf;
	}

	public static HashMap<Integer, LinkedHashMap<Integer, Double>> getScoresTableIntIntDouble(Connection con, String in_query) throws SQLException{

		//put words_count_tfidf table into nested hashmap, linked hashmap, linked hashmap is ordered ordered by tfidf desc
		Statement stWordsCountTfidf= con.createStatement();
		ResultSet rsWordsCountTfidf= stWordsCountTfidf.executeQuery(in_query);
		HashMap<Integer, LinkedHashMap<Integer, Double>> wordsCountTfidf= new HashMap<Integer, LinkedHashMap<Integer,Double>>();
		while(rsWordsCountTfidf.next()){
			int currentDocId=rsWordsCountTfidf.getInt(1);
			int currentWordId=rsWordsCountTfidf.getInt(2);
			double currentTfidf=rsWordsCountTfidf.getDouble(3);
			if(wordsCountTfidf.containsKey(currentDocId)){
				wordsCountTfidf.get(currentDocId).put(currentWordId, currentTfidf);
			}
			else{
				wordsCountTfidf.put(currentDocId, new LinkedHashMap<Integer, Double>());
				wordsCountTfidf.get(currentDocId).put(currentWordId, currentTfidf);
			}
		}
		rsWordsCountTfidf.close();
		stWordsCountTfidf.close();

		return wordsCountTfidf;
	}


	

	public static HashMap<Integer, HashSet<String>> getTwoColumnsIntStringContainerSetOfItems(Connection con, String in_sourceTable, String in_containerIdField, String in_itemIdField) throws Exception{
		String sourceTable=in_sourceTable;
		String containerIdField=in_containerIdField;
		String itemIdField=in_itemIdField;

		Statement st= con.createStatement();
		ResultSet rs= st.executeQuery("select " + containerIdField + ", " + itemIdField + " from " + sourceTable + " order by " + containerIdField + ";");
		HashMap<Integer, HashSet<String>> res= new HashMap<Integer, HashSet<String>>();
		while(rs.next()){
			int currentId=rs.getInt(containerIdField);
			String currentWord= rs.getString(itemIdField);
			if(res.containsKey(currentId)){
				res.get(currentId).add(currentWord);
			}
			else{
				res.put(currentId, new HashSet<String>());
				res.get(currentId).add(currentWord);
			}
		}
		rs.close();
		st.close();

		return res;
	}

	public static HashMap<Integer, HashMap<Integer, Integer>> getTwoIntColumnsContainerSetOfItemsWithDuplicateCount(Connection con, String in_sourceTable, String in_containerIdField, String in_itemIdField) throws Exception{
		String sourceTable=in_sourceTable;
		String containerIdField=in_containerIdField;
		String itemIdField=in_itemIdField;

		Statement st= con.createStatement();
		ResultSet rs= st.executeQuery("select " + containerIdField + ", " + itemIdField + " from " + sourceTable + " order by " + containerIdField + ";");
		HashMap<Integer, HashMap<Integer, Integer>> res= new HashMap<Integer, HashMap<Integer,Integer>>();

		while(rs.next()){
			int currentId=rs.getInt(containerIdField);
			int currentWordId= rs.getInt(itemIdField);
			int count=1;
			if(res.containsKey(currentId)){
				if(res.get(currentId).containsKey(currentWordId)){
					count=res.get(currentId).get(currentWordId)+1; 
				}
			}
			else{
				res.put(currentId, new HashMap<Integer, Integer>());
			}
			res.get(currentId).put(currentWordId, count);
		}
		rs.close();
		st.close();

		return res;
	}



	public static HashMap<Integer, String> getTwoColumnsIntStringDistinctPairs(Connection con, String in_sourceTable, String in_KeyField, String in_itemField) throws Exception{
		String sourceTable=in_sourceTable;
		String keyField=in_KeyField;
		String itemField=in_itemField;

		Statement st= con.createStatement();
		ResultSet rs= st.executeQuery("select " + keyField + ", " + itemField + " from " + sourceTable + " order by " + keyField + ";");
		HashMap<Integer, String> res= new HashMap<Integer, String>();
		while(rs.next()){
			int currentKey=rs.getInt(keyField);
			String currentItem= rs.getString(itemField);
			if(res.containsKey(currentKey)){
				System.out.println("ERROR: getTwoIntColumnsDistinctPairs: duplicate entry for key field " + keyField + ": " + currentKey);
				return null;
			}
			else{
				res.put(currentKey, currentItem);
			}
		}
		rs.close();
		st.close();

		return res;
	}

	public static HashMap<Integer, Double> getTwoColumnsIntDoubleDistinctPairs(Connection con, String in_sourceTable, String in_KeyField, String in_itemField) throws Exception{
		String sourceTable=in_sourceTable;
		String keyField=in_KeyField;
		String itemField=in_itemField;

		Statement st= con.createStatement();
		ResultSet rs= st.executeQuery("select " + keyField + ", " + itemField + " from " + sourceTable);
		HashMap<Integer, Double> res= new HashMap<Integer, Double>();
		while(rs.next()){
			int currentKey=rs.getInt(keyField);
			double currentItem= rs.getDouble(itemField);
			if(res.containsKey(currentKey)){
				System.out.println("ERROR: getTwoIntColumnsDistinctPairs: duplicate entry for key field " + keyField + ": " + currentKey);
				return null;
			}
			else{
				res.put(currentKey, currentItem);
			}
		}
		rs.close();
		st.close();

		return res;
	}

	public static HashMap<String, Double> getTwoColumnsStringDoubleDistinctPairs(Connection con, String in_query) throws Exception{

		Statement st= con.createStatement();
		ResultSet rs= st.executeQuery(in_query);
		HashMap<String, Double> res= new HashMap<String, Double>();
		while(rs.next()){
			String currentKey=rs.getString(1);
			double currentItem= rs.getDouble(2);
			if(res.containsKey(currentKey)){
				System.out.println("ERROR: getTwoIntColumnsDistinctPairs: duplicate entry: (" + currentKey + ", " + currentItem + ")");
				return null;
			}
			else{
				res.put(currentKey, currentItem);
			}
		}
		rs.close();
		st.close();

		return res;
	}

	 */
}