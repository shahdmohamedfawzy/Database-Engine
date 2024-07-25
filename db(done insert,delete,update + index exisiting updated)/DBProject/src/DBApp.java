
/** * @author Wael Abouelsaadat */ 

import java.io.*;
import java.util.*;

import com.opencsv.CSVReader;


public class DBApp {

	
	//public static ArrayList<Table> tables=new ArrayList<Table>();
	public static Vector<Table> tables=new Vector<Table>();
	
	public DBApp( ){
		
	}

	// this does whatever initialization you would like 
	// or leave it empty if there is no code you want to 
	// execute at application startup 
	public void init( ){
		try{
			FileInputStream fileIn = new FileInputStream("tablesVector.class");
		
			 ObjectInputStream objectIn  = new ObjectInputStream(fileIn);
			 Object obj;
				try {
				while ((obj = objectIn.readObject()) != null) {
					if(obj instanceof Vector) {
						tables=(Vector<Table>)obj;
						
					}
				}
				}catch(EOFException e) {
					System.out.println("deserialization of tables done");
					
					
				}
			
   
	}catch(Exception e) {
		System.out.println(e.getMessage());
	}}


	// following method creates one table only
	// strClusteringKeyColumn is the name of the column that will be the primary
	// key and the clustering column as well. The data type of that column will
	// be passed in htblColNameType
	// htblColNameValue will have the column name as key and the data 
	// type as value
	public void createTable(String strTableName, 
							String strClusteringKeyColumn,  
							Hashtable<String,String> htblColNameType) throws DBAppException, Exception{
		
		//check table name doesn't exist
		//check table doesn't contain 0 cols
		
		try {
			checkCreation(strTableName, htblColNameType);
			
		}catch(DBAppException e) {
			System.out.println(e.getMessage());
			return;
			
		}
		
		
		Table t1=new Table(strTableName,strClusteringKeyColumn,htblColNameType);
		DBApp.tables.add(t1);
		
		
		 
		//clearing the page content, to add it all over again 
		try (FileOutputStream fileOutputStream = new FileOutputStream("tablesVector.class")) {
            fileOutputStream.getChannel().truncate(0);
        }

		//serializing the page again
		FileOutputStream fileOut = new FileOutputStream("tablesVector.class");
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(DBApp.tables);
        
        //resource closing
        
        out.close();
        fileOut.close();
								
		
	}
	
	
	


	// following method creates a B+tree index 
	public void createIndex(String   strTableName,
							String   strColName,
							String   strIndexName) throws DBAppException,Exception{
		
		
		
		//check validity from csv
		try {
			checkDataIndex(strTableName, strColName);
			
		}catch(DBAppException e) {
			System.out.println(e.getMessage());
			return;
		}
		
		Index i= new Index(strIndexName,strColName);
		i.bt=new bplustree(3);//assumption en node can carry max of 3 keys
		
		Table table=null;
		for(Table t : DBApp.tables ) {
			if(t.tableName==strTableName) {
				table=t;
				System.out.println(table.tableName);
				break;
			}
			
		}
		
		for(String pagePath:table.pageCount.keySet()) {
			//get all records in this page and add it to bt
			Object obj;
			try {
		         FileInputStream fileIn = new FileInputStream(pagePath);
		         ObjectInputStream objectIn  = new ObjectInputStream(fileIn);
		         try {
			         while ((obj = objectIn.readObject()) != null) {
			                if (obj instanceof Page) {
			                    
			                    Page page=(Page) obj;
			                    Object toBeIndexed=null;
			                    for(Record record:page.records) {
			                    	for(int j=0;j<record.record.size();j++) {
				                    	
				                    	if(record.record.get(j) instanceof String) {
				                    		if(((String)record.record.get(j))==strColName) {
				                    			toBeIndexed=record.record.get(j+1);
				                    			i.bt.insert(toBeIndexed,page.number );
				                    			break;//break ashan khalas laeet column el bdwr aleh
				                    		}
				                    	}
				                    	
				                    }//loop on record vector	
			                    }//loop on records of page
			                    
			                }
			            }}catch(EOFException e) {
							 System.out.println("reading from file in index is done");
					    	  
					      }

		            objectIn.close();
		            fileIn.close();
		            String indexFilePath=strIndexName+".class";
		            table.index2.add(indexFilePath);
		            FileOutputStream fileOut = new FileOutputStream(indexFilePath);//serialize in index name path
		            ObjectOutputStream out = new ObjectOutputStream(fileOut);
		            out.writeObject(i);
		        	out.close();
		        	fileOut.close();
		        	Table.rewriteCSVLine(strTableName,strColName,strIndexName);
		         
		         
		      } catch (IOException e) {
		         e.printStackTrace();
		         return;
		      } catch (ClassNotFoundException c) {
		         System.out.println("class not found");
		         c.printStackTrace();
		         return;
		      }
			
			
		}
		
		
	}


	// following method inserts one row only. 
	// htblColNameValue must include a value for the primary key
	public void insertIntoTable(String strTableName, 
								Hashtable<String,Object>  htblColNameValue) throws DBAppException, Exception{
		
		//check metadata.csv
		try {
			checkDataInsertion(htblColNameValue, strTableName);
		}catch(DBAppException e) {
			System.out.println(e.getMessage());
			return;
		}
		
		Table table=null;
		for(Table t:DBApp.tables) {
			System.out.println(t.tableName);
			if(t.tableName.equals( strTableName)) {
				table=t;
				System.out.println("inside loop" +table.tableName);
				break;
			}
		}
		table.insertIntoTable( htblColNameValue);
		
	
	}


	// following method updates one row only
	// htblColNameValue holds the key and new value 
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the row to update.
	public void updateTable(String strTableName, 
							String strClusteringKeyValue,
							Hashtable<String,Object> htblColNameValue   )  throws Exception{
		//check metadata
		try {
			checkDataUpdate(htblColNameValue, strTableName, strClusteringKeyValue);
			
		}catch(DBAppException e) {
			System.out.println(e.getMessage());
			return;
		}
		Table table=null;
		for(Table t:DBApp.tables) {
			if(t.tableName.equals( strTableName)) {
				table=t;
				break;
			}
		}
		table.updateTable(strClusteringKeyValue, htblColNameValue);
	
		
	}


	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search 
	// to identify which rows/tuples to delete. 	
	// htblColNameValue enteries are ANDED together
	public void deleteFromTable(String strTableName, 
								Hashtable<String,Object> htblColNameValue) throws Exception{
		//check metadata
		try {
			checkDataDeletion(htblColNameValue, strTableName);
			
		}catch(DBAppException e) {
			System.out.println(e.getMessage());
			return;
		}
		Table table=null;
		for(Table t:DBApp.tables) {
			if(t.tableName.equals( strTableName)) {
				table=t;
				break;
			}
		}
		table.deleteFromTable(htblColNameValue);
	
		
	}


	public Iterator<?> selectFromTable(SQLTerm[] arrSQLTerms, 
			String[]  strarrOperators) throws DBAppException,Exception{
		//check that table exists
		String tableName2="";
		for(SQLTerm termf:arrSQLTerms) {
			
			String tableName = termf._strTableName;
			String colName=termf._strColumnName;
			Object value=termf._objValue;
			try {
				checktableExist(tableName,colName,value);
			}catch(DBAppException e) {
				System.out.println(e.getMessage());
				return null;
			}
			tableName2=tableName;
		
		}
			Table table=null;
		
			for(Table t:DBApp.tables) {
				if(t.tableName.equals( tableName2)) {
					table=t;
					break;
				}
			
			}
			
			
			return table.selectFromTable(arrSQLTerms, strarrOperators);
		}
	
	
	
	
	public boolean checkCreation(String strTableName,Hashtable<String,String> htblColNameType)throws Exception {
		CSVReader csvReader = new CSVReader(new FileReader("metadata.csv"));
		
        // Read all records from the CSV file
        List<String[]> allRecords = csvReader.readAll();
        csvReader.close();
        for (String[] record : allRecords) {
            if (record[0].equals(strTableName)) {
            	throw new DBAppException("TABLE already EXIST");
            }
        }
        
		if(htblColNameType.isEmpty())
			throw new DBAppException("table has no columns");
		
		
		return true;
		
	}
	
	
	public boolean checktableExist(String tableName, String colName , Object value )throws Exception {
		 
			    boolean colExists=false;
				boolean tableExists=false;
				CSVReader csvReader = new CSVReader(new FileReader("metadata.csv"));
		        // Read all records from the CSV file
		        List<String[]> allRecords = csvReader.readAll();
		        csvReader.close();
		        ArrayList<String[]> tableData= new ArrayList<String[]>();
		        for (String[] record : allRecords) {
		            if (record[0].equals(tableName)) {
		                tableData.add(record);
		                tableExists=true;  
		            }
		        }
		        if (!tableExists) {
	                throw new DBAppException("Table '" + tableName + "' does not exist.");
	            }
		        for(String[] record : tableData) {
		    		   if(record[1].equals(colName)) {
		    			   colExists=true;
		    			   if (!isValidDataType(value, record[2])) {
		                        throw new DBAppException("Invalid data type for column '" + colName + "'.");
		                    }
		    		   }
		        }  
		        if (!colExists) {
	                throw new DBAppException("Column '" + colName + "' does not exist in table '" + tableName + "'.");
	            }
		        
	        return true;
	}
	
	
	
	

	
	
	public boolean checkDataUpdate(Hashtable<String,Object>  htblColNameValue,String tableName,String strClusteringKeyValue) throws Exception {
		CSVReader csvReader = new CSVReader(new FileReader("metadata.csv"));
		
        // Read all records from the CSV file
        List<String[]> allRecords = csvReader.readAll();
        csvReader.close();
        ArrayList<String[]> tableData= new ArrayList<String[]>();
        // Search for the line to rewrite based on the table name and column name
        boolean tableExists=false;
        for (String[] record : allRecords) {
        
            if (record[0].equals(tableName)) {
                tableData.add(record);
                tableExists=true;
                
            }
        }
        if(tableExists==false)
        	throw new DBAppException("TABLE DOESN'T EXIST");
        
        
       boolean colExists=false;
       
       for(String key:htblColNameValue.keySet()) { //Check the column wanting to be updated is in the csv
    	   colExists=false;
    	   for(String[] record : tableData) {
    		   if(key.equals(record[1])) {
    			   colExists=true;
    			   break;
    		   }
    	   }
    	   if(colExists==false) {
    		   throw new DBAppException("COLUMN DOESN'T EXIST");//user mdkhl col msh mawgood
    	   }
    	   
       }
       //for getting the type of entered clstring key
       Object clstr=null;
       boolean integer=true;
       boolean ddouble=false;
       boolean string=false;
       
       try {
    	   clstr=Integer.parseInt(strClusteringKeyValue);
    	   
       }catch(Exception e) {
    	   try {
    		   integer=false;
    		   ddouble=true;
    		   clstr=Double.parseDouble(strClusteringKeyValue);
    	   }catch(Exception ee) {
    		   ddouble=false;
    		   string=true;
    	   }  
       }
       Object clusteringKey=null;
       if(integer) {
    	   clusteringKey=Integer.parseInt(strClusteringKeyValue);
    	   
       }else if(ddouble) {
    	   clusteringKey=Double.parseDouble(strClusteringKeyValue);
    	   
       }else {
    	   clusteringKey=strClusteringKeyValue;
    	   
       }
       
       
       for(String[]record:tableData) {
    	   if(record[3].equals("True")) {//checking clstringkey data type
    		   if(isValidDataType(clusteringKey,record[2])==false)
    			   throw new DBAppException("INVALID clustering key COLUMN TYPE ENTRY");//wrong data type
    		   else
    			   break;
    	   }  
       }
       
       
              
       //need to check each data type entered is valid
       for(String column:htblColNameValue.keySet()) {
    	   for(String[] record : tableData) {
    		   if(record[1].equals(column) && isValidDataType(htblColNameValue.get(column),record[2])==false) {
    			   throw new DBAppException("INVALID DATA COLUMN TYPE ENTRY");//wrong data type 
    			   
    		   }  
    	   }   
       }
       
	return true;	
	}
	
	public boolean checkDataDeletion(Hashtable<String,Object>  htblColNameValue,String tableName) throws Exception {
		CSVReader csvReader = new CSVReader(new FileReader("metadata.csv"));
		
        // Read all records from the CSV file
        List<String[]> allRecords = csvReader.readAll();
        csvReader.close();
        ArrayList<String[]> tableData= new ArrayList<String[]>();
        // Search for the line to rewrite based on the table name and column name
        boolean tableExists=false;
        for (String[] record : allRecords) {
        
            if (record[0].equals(tableName)) {
                tableData.add(record);
                tableExists=true;
                
            }
        }
        if(tableExists==false)
        	throw new DBAppException("TABLE DOESN'T EXIST");
        
        
       boolean colExists=false;
       
       for(String key:htblColNameValue.keySet()) { //Check all columns entered are in the csv
    	   colExists=false;
    	   for(String[] record : tableData) {
    		   if(key.equals(record[1])) {
    			   colExists=true;
    			   
    			   break;
    		   }
    	   }
    	   if(colExists==false) {
    		   throw new DBAppException("COLUMN DOESN'T EXIST");//user mdkhl col msh mawgood
    	   }
    	   
       }
       
              
       //need to check each data type entered is valid
       for(String column:htblColNameValue.keySet()) {
    	   for(String[] record : tableData) {
    		   if(record[1].equals(column) && isValidDataType(htblColNameValue.get(column),record[2])==false) {
    			   throw new DBAppException("INVALID DATA COLUMN TYPE ENTRY");//wrong data type 
    			   
    		   }  
    	   }   
       }
       
	return true;	
	}
	//fih hetta na2sa eny a check index doesn't exist
	public boolean checkDataIndex(String tableName,String   strColName) throws Exception {
		CSVReader csvReader = new CSVReader(new FileReader("metadata.csv"));
		
        // Read all records from the CSV file
        List<String[]> allRecords = csvReader.readAll();
        csvReader.close();
        ArrayList<String[]> tableData= new ArrayList<String[]>();
        // Search for the line to rewrite based on the table name and column name
        boolean tableExists=false;
        for (String[] record : allRecords) {
        
            if (record[0].equals(tableName)) {
                tableData.add(record);
                tableExists=true;
                
            }
        }
        if(tableExists==false)
        	throw new DBAppException("TABLE DOESN'T EXIST");
        
        
       boolean colExists=false;
       
       
    	   for(String[] record : tableData) {
    		   if(strColName.equals(record[1])) {
    			   colExists=true;
    			   if(!(record[4].equals("null"))) {
    				   throw new DBAppException("index already exists");
    			   }
    			   break;
    		   }
    	   }
    	   if(colExists==false) {
    		   throw new DBAppException("COLUMN DOESN'T EXIST");//user mdkhl col msh mawgood
    	   }
    	    
	return true;	
	}
	
	
	public boolean checkDataInsertion(Hashtable<String,Object>  htblColNameValue,String tableName) throws Exception {
		CSVReader csvReader = new CSVReader(new FileReader("metadata.csv"));
		
        // Read all records from the CSV file
        List<String[]> allRecords = csvReader.readAll();
        csvReader.close();
        ArrayList<String[]> tableData= new ArrayList<String[]>();
        // Search for the line to rewrite based on the table name and column name
        boolean tableExists=false;
        for (String[] record : allRecords) {
        
            if (record[0].equals(tableName)) {
                tableData.add(record);
                tableExists=true;
                
            }
        }
        if(tableExists==false)
        	throw new DBAppException("TABLE DOESN'T EXIST");
        
        
       boolean colExists=false;
       int TableCols=tableData.size();
       for(String key:htblColNameValue.keySet()) { //Check all columns entered are in the csv
    	   colExists=false;
    	   for(String[] record : tableData) {
    		   if(key.equals(record[1])) {
    			   colExists=true;
    			   TableCols--;
    			   break;
    		   }
    	   }
    	   if(colExists==false) {
    		   throw new DBAppException("COLUMN DOESN'T EXIST");//user mdkhl col msh mawgood
    	   }
//    	   if(TableCols>0) {
//    		   throw new DBAppException("Null column entry");
//    	   }
       }
       
              
       //need to check each data type entered is valid
       for(String column:htblColNameValue.keySet()) {
    	   for(String[] record : tableData) {
    		   if(record[1].equals(column) && isValidDataType(htblColNameValue.get(column),record[2])==false) {
    			   throw new DBAppException("INVALID DATA COLUMN TYPE ENTRY");//wrong data type 
    			   
    		   }  
    	   }   
       }
       
	return true;	
	}
	
	
	
									// val to be inserted		type from csv
	public boolean isValidDataType(Object dataFromTable, String columnType) {
	    String dataType = ""; 
	    
	    if(columnType.equals( "java.lang.Integer")) {
	    	dataType = "Integer";
	    }else if(columnType.equals( "java.lang.String")) {
	    	
	    	if(dataFromTable instanceof String) {
	    		
	    		return true;
	    	}
	    	dataType = "String";
	    }else if(columnType .equals("java.lang.Double")) {
	    	dataType = "Double";
	    }else {
	        // Print columnType for debugging purposes
	        System.out.println("Unexpected column type: " + columnType);
	    }
	    
	    // Check if the value is of the expected data type
	    try {
	    	
	        Class<?> c = Class.forName("java.lang." + dataType);
	        //System.out.println("return of isvalid without catch is"+c.isInstance(dataFromTable));
	        return c.isInstance(dataFromTable);
	    } catch (ClassNotFoundException e) {
	        // Handle if the data type class is not found
	        e.printStackTrace();
	        return false;
	    }
	}
	


	public static void main( String[] args ){
	
	try{
		 DBApp	dbApp = new DBApp( );
		 dbApp.init();
		
			String strTableName = "Student";
//			if(DBApp.tables.isEmpty()) {
//				System.out.println("empty");
//			}else {
//				System.out.println(tables.size());
//			}
			
			Table t=tables.get(0);
			//System.out.println(t.getAllRecords());
			
//			Hashtable htblColNameType = new Hashtable( );
//			htblColNameType.put("id", "java.lang.Integer");
//			htblColNameType.put("name", "java.lang.String");
//			htblColNameType.put("gpa", "java.lang.Double");
//			dbApp.createTable( strTableName, "id", htblColNameType );
			dbApp.createIndex( strTableName, "gpa", "gpaIndex" );
//
//			
//			
			Hashtable htblColNameValue = new Hashtable( );
		
//			htblColNameValue.put("id", (int) 2343432 );
//			htblColNameValue.put("name", new String("Ahmed")  );
//			htblColNameValue.put("gpa", (Double) 0.95  );
//			dbApp.insertIntoTable( strTableName , htblColNameValue );
//			
//			htblColNameValue.put("id", (int) 2 );
//			htblColNameValue.put("name", new String("Ahmed")  );
//			htblColNameValue.put("gpa", (Double) 0.95  );
//			dbApp.insertIntoTable( strTableName , htblColNameValue );
////			
//			htblColNameValue.put("id", (int) 3 );
//			htblColNameValue.put("name", new String("Dalia")  );
//			htblColNameValue.put("gpa", (Double) 0.95  );
//			dbApp.insertIntoTable( strTableName , htblColNameValue );
//			
//			htblColNameValue.put("id", (int) 7 );
//			htblColNameValue.put("name", new String("khaled")  );
//			htblColNameValue.put("gpa", (Double) 0.95  );
//			dbApp.insertIntoTable( strTableName , htblColNameValue );
			
//			System.out.println(""+t.getAllRecords());
//			
//			
//			
//			//update testing
//			htblColNameValue.clear();
//			htblColNameValue.put("name", "Yara");
//			dbApp.updateTable(strTableName, "2", htblColNameValue);
//			
			
			
			
			
			
			
			
			
			
			
			
			//deletion testing
//			htblColNameValue.clear();
//			//dbApp.deleteFromTable(strTableName, htblColNameValue);
//			//htblColNameValue.put("name", "Ahmed");
//			htblColNameValue.put("name", "Ahmed");
//			htblColNameValue.put("id", 2);
//			
			//System.out.println("before delete \n"+t.getAllRecords());
			//dbApp.deleteFromTable(strTableName, htblColNameValue);
			
//			htblColNameValue.clear( );
//			htblColNameValue.put("id",  453455 );
//			htblColNameValue.put("name", "Ahmed Noor"  );
//			htblColNameValue.put("gpa", (Double) 0.95  );
//			dbApp.insertIntoTable( strTableName , htblColNameValue );
//
//			htblColNameValue.clear( );
//			htblColNameValue.put("id", (int) 5674567 );
//			htblColNameValue.put("name", "Dalia Noor"  );
//			htblColNameValue.put("gpa", (Double) 1.25  );
//			dbApp.insertIntoTable( strTableName , htblColNameValue );
			
			
			//System.out.println("after delete \n"+t.getAllRecords());
			
//
//			htblColNameValue.clear( );
//			htblColNameValue.put("id",  23498 );
//			htblColNameValue.put("name", new String("John Noor" ) );
//			htblColNameValue.put("gpa", (Double) 1.5  );
//			dbApp.insertIntoTable( strTableName , htblColNameValue );
//
//			htblColNameValue.clear( );
//			htblColNameValue.put("id",  78452 );
//			htblColNameValue.put("name", new String("Zaky Noor" ) );
//			htblColNameValue.put("gpa", (Double) 0.88  );
//			dbApp.insertIntoTable( strTableName , htblColNameValue );


//			SQLTerm[] arrSQLTerms;
//			arrSQLTerms = new SQLTerm[2];
//			arrSQLTerms[0]._strTableName =  "Student";
//			arrSQLTerms[0]._strColumnName=  "name";
//			arrSQLTerms[0]._strOperator  =  "=";
//			arrSQLTerms[0]._objValue     =  "John Noor";
//
//			arrSQLTerms[1]._strTableName =  "Student";
//			arrSQLTerms[1]._strColumnName=  "gpa";
//			arrSQLTerms[1]._strOperator  =  "=";
//			arrSQLTerms[1]._objValue     =  new Double( 1.5 );
//
//			String[]strarrOperators = new String[1];
//			strarrOperators[0] = "OR";
//			// select * from Student where name = "John Noor" or gpa = 1.5;
//			Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);
		}
		catch(Exception exp){
			exp.printStackTrace( );
		}
	}

}