import java.io.*;
import java.util.*;

import com.opencsv.*;




public class Table implements Serializable{

	//sarah idk
	private static final long serialVersionUID = 1L;
	String tableName;
	String strClusteringKeyColumn;
	Hashtable<String,String> htblColNameType=new Hashtable<String,String>();
	int maxPageTuples;
	Hashtable<String, Integer> pageCount = new Hashtable<>();
	
	
	ArrayList<String> index2=new ArrayList<String>();
	
	
	ArrayList<Integer> pages=new ArrayList<Integer>();
	
	
	//methods to use:
	// 				Double search
	
	//key = index el handwr aleh
	//value = page number
	//take care:
			//page class utilization may be done better , enta amelha betb3t el filename fel function msh msstkhdm 
			//constructor el page
	//edit to use pageCount.size wa remove ay haga fiha pages.size
	
	
	
	public Table(String name,String strClusteringKeyColumn,  
			Hashtable<String,String> htblColNameType) throws IOException {
		this.tableName=name;
		this.strClusteringKeyColumn=strClusteringKeyColumn;
		
		for (String columnName : htblColNameType.keySet()) {
            String columnType = htblColNameType.get(columnName);
            System.out.println("Column: " + columnName + ", Data type: " + columnType);
            this.htblColNameType.put(columnName, columnType);
        }
		
		pages.add(1);
		String fileName=tableName+"1"+".class";
		pageCount.put(fileName, 0);
		
		Page page=new Page(fileName,1);
		 //serilaize el page hena abl ma te call el fn ashan hatbaa error
		 FileOutputStream fileOut = new FileOutputStream(fileName);
	        ObjectOutputStream out = new ObjectOutputStream(fileOut);
	        out.writeObject(page);
	        
	        //resource closing
	        out.close();
	        fileOut.close();
	        
	        // data to be written to the CSV file
	        for(String columnName : htblColNameType.keySet()) {
	        	
	        	String[] s=new String[6];
	        	if(columnName.equals(strClusteringKeyColumn)) {
	        		
	        		s[0]=name;
	        		s[1]=columnName;
	        		s[2]=htblColNameType.get(columnName);
	        		s[3]="True";
	        		s[4]="null";
	        		s[5]="null";
	        	}else {
	        		
	        		s[0]=name;
	        		s[1]=columnName;
	        		s[2]=htblColNameType.get(columnName);
	        		s[3]="False";
	        		s[4]="null";
	        		s[5]="null";
	        }
	        	
	        	insertTableToCSV(s);
	        }
	        
	        
	}
	
	
	
	public void insertIntoTable( 
			Hashtable<String,Object>  htblColNameValue)throws DBAppException, IOException,Exception {
		
		//check data entering is correct from metadata.csv
		
		//getting max for page
		BufferedReader br=new BufferedReader(new FileReader("DBApp.config"));
		String line="";
		try {
			while((line=br.readLine())!=null){
				String[]reading=line.split("="); 
				maxPageTuples=Integer.parseInt(reading[1]);
				
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//got page max, proceed with insertion
			//check if last page isn't full
				//if a page is found , insert in it
				//else create a new page and insert into hashtable
		
		
		//adding data to tuple vector
		Record newTuple=new Record(htblColNameValue);
		
		//check data types from csv file 
		
		
		
		
		String fileName;
		int lastIndex=pages.size();
		String lastPage=tableName+""+lastIndex+".class";
		
		int lastIndexVal =pageCount.get(lastPage);
		if(lastIndexVal == maxPageTuples) {
			//create a new page
			
			 pages.add(pages.size()+1);
			 lastIndex=pages.size();
			 
			 fileName=tableName+""+lastIndex+".class";
			 Page page=new Page(fileName, lastIndex);
			 //serilaize el page hena abl ma te call el fn ashan hatbaa error
			 FileOutputStream fileOut = new FileOutputStream(fileName);
		        ObjectOutputStream out = new ObjectOutputStream(fileOut);
		        out.writeObject(page);
		        
		        //resource closing
		        out.close();
		        fileOut.close();
			 
			 addToPage(fileName, newTuple);
			 
			 pageCount.put(fileName, 1);
			 
			
		}else {
			//insert into existing page
			fileName=tableName+""+lastIndex+".class";
			
	         
			int lastVal=pageCount.get(fileName);
			 pageCount.put(fileName, lastVal+1);
			 
			 addToPage(fileName, newTuple);
			 
		}
		
		//if exists indicies add values to them
		for(String colName:htblColNameValue.keySet()) {
			 if(isIndexed(colName)) {
				 insertIntoTree(colName,htblColNameValue.get(colName), lastIndex);
				 
			 }
			 
		 }
		System.out.println("Insertion done");

		
		
	}
	
	
	//handle deleting all from table, hashtable is empty
	public void deleteFromTable
	(Hashtable<String,Object> htblColNameValue) throws Exception{
		//assume that deletion condition will always be haga=haga
		
		
		if(htblColNameValue.isEmpty()) {
			//truncating pages
			for(String p:pageCount.keySet()) {
				FileInputStream fileIn = new FileInputStream(p);
				ObjectInputStream objectIn  = new ObjectInputStream(fileIn);
				Object obj;
				//reading from file vector by vector
				try {
				while ((obj = objectIn.readObject()) != null) {
					if(obj instanceof Page) {
						Page page=(Page)obj;
						page.records.removeAllElements();
						
						try (FileOutputStream fileOutputStream = new FileOutputStream(p)) {
				            fileOutputStream.getChannel().truncate(0);
				        }
						
						FileOutputStream fileOut = new FileOutputStream(p);
				        ObjectOutputStream out = new ObjectOutputStream(fileOut);
				        
				        out.writeObject(page);
				       
				        out.close();
				        fileOut.close();
					
					}
					}}catch(EOFException e) {
						System.out.println(" End of reading file in delete 1");
					}
				}
			
			//truncating index
			for(String indexFilePath: index2) {
				Index i=null;
				FileInputStream fileIn1 = new FileInputStream(indexFilePath);
				ObjectInputStream objectIn1  = new ObjectInputStream(fileIn1);
				Object obj1;
				try {
				while((obj1 = objectIn1.readObject()) != null) {
					if(obj1 instanceof Index) {
						i=(Index)obj1;
						i.bt=new bplustree(3);
						
						objectIn1.close();
						fileIn1.close();
						
						try (FileOutputStream fileOutputStream = new FileOutputStream(indexFilePath)) {
				            fileOutputStream.getChannel().truncate(0);
				        }
						
						//adding updated
						FileOutputStream fileOut = new FileOutputStream(indexFilePath);
				        ObjectOutputStream out = new ObjectOutputStream(fileOut);
				        
				        out.writeObject(i);
				       
				        out.close();
				        fileOut.close();
					}
				
				}}catch(EOFException e) {
					System.out.println(" End of reading file in delete 2");
				}
			}
			
			
			
			
			
		}else {
		
		int conditions=htblColNameValue.size();
		
		boolean Indexed=false;
		String indexedCol="";
		//getting the indexed column 
		for(String colName:htblColNameValue.keySet()) {
			if(isIndexed(colName)) {
				Indexed=true;
				indexedCol=colName;
				break;
			}
			else {
				Indexed=false;
			}
		}
		if(!Indexed) {
			for(String p:pageCount.keySet()) {
				deleteRecords(p, htblColNameValue, conditions);
				
				}//end of looping on pages
			}
			else{ 
				//indexed
				bplustree btI=getTree(indexedCol);
				ArrayList<Integer> pageDeletions=btI.getAllValues(htblColNameValue.get(indexedCol));
				
				for(int PageNumber:pageDeletions) {
					String p=tableName+""+PageNumber+".class";
					deleteRecords(p, htblColNameValue, conditions);
				}
			  }}
			
	}//fn bracket
	

	
	
	
	public void updateTable(
			String strClusteringKeyValue,
			Hashtable<String,Object> htblColNameValue   )  throws DBAppException, IOException, ClassNotFoundException,Exception{
		
		String key="";
		for(String k:htblColNameValue.keySet()) {
			key=k;
		}
		boolean thisRecord=false;
		
		if(!isIndexed(strClusteringKeyValue)) {
			
			for(String page:pageCount.keySet() ) {
				if(thisRecord==true)
					break;
				//readFromPage(page);
				thisRecord=updateRecords(strClusteringKeyValue, htblColNameValue, page, key);
				
			}
			
		
		}//end of not indexed
		else {
			bplustree bt=getTree(strClusteringKeyValue);
			double p= bt.search(strClusteringKeyValue);
			int PageNumber =(int) p;
			String page=tableName+""+PageNumber+".class";
			thisRecord=updateRecords(strClusteringKeyValue, htblColNameValue, page, key);
			
			
			
			
			
			
		
		throw new DBAppException("not implemented yet");
			}
		}
	

	public boolean isIndexed(String colName)throws Exception {
		for(String indexFilePath: index2) {
			
			FileInputStream fileIn = new FileInputStream(indexFilePath);
			ObjectInputStream objectIn  = new ObjectInputStream(fileIn);
			Object obj;
			try {
			while((obj = objectIn.readObject()) != null) {
				if(obj instanceof Index) {
					Index i=(Index)obj;
					if(i.colName==colName) {
						objectIn.close();
						fileIn.close();
						return true;
				}}
					
				}}catch(EOFException e) {
					System.out.println("Reading done of index in table class");
				}
			objectIn.close();
			fileIn.close();
			}
		
		return false;
			
	}

	public bplustree getTree (String colName)throws Exception {
		for(String indexFilePath: index2) {
			
			FileInputStream fileIn = new FileInputStream(indexFilePath);
			ObjectInputStream objectIn  = new ObjectInputStream(fileIn);
			Object obj;
			try {
			while((obj = objectIn.readObject()) != null) {
				if(obj instanceof Index) {
					Index i=(Index)obj;
					if(i.colName==colName) {
						objectIn.close();
						fileIn.close();
						return i.bt;
				}}
					
				}}catch(EOFException e) {
					System.out.println(" End of reading file in getTree");
				}
			objectIn.close();
			fileIn.close();
			}
			
			
		return null;
	}
	
	public void insertIntoTree(String colName,Object key,int PageNum)throws Exception {
		bplustree bt=null;
		Index i=null;
		String ifp="";
		for(String indexFilePath: index2) {
			
			FileInputStream fileIn = new FileInputStream(indexFilePath);
			ObjectInputStream objectIn  = new ObjectInputStream(fileIn);
			Object obj;
			try {
			while((obj = objectIn.readObject()) != null) {
				if(obj instanceof Index) {
					 i=(Index)obj;
					if(i.colName==colName) {
						objectIn.close();
						fileIn.close();
						bt= i.bt;
						ifp=indexFilePath;
					}
				}
					
			}}catch(EOFException e) {
				System.out.println(" End of reading file in insertIntoTree");
			}
			objectIn.close();
			fileIn.close();
		}
		bt.insert(key, PageNum);
		try (FileOutputStream fileOutputStream = new FileOutputStream(ifp)) {
            fileOutputStream.getChannel().truncate(0);
        }
		
		//adding updated
		FileOutputStream fileOut = new FileOutputStream(ifp);
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        
        out.writeObject(i);
       
        out.close();
        fileOut.close();
		
		
	}
	
	public void readFromPage(String p) throws Exception {
		FileInputStream fileIn = new FileInputStream(p);
		ObjectInputStream objectIn  = new ObjectInputStream(fileIn);
		Object obj;
		int recordIndex=0;
		int ix=0;
		Record FinalRec=null;
		System.out.println(p);
		try {
		while((obj = objectIn.readObject()) != null) {
			if(obj instanceof Page) {
				Page page=(Page)obj;
				for(Record record:page.records) {
					System.out.println(record.toString());
				}
	}}}catch(EOFException e) {
		System.out.println(e.getMessage());

	
	}}
	
	
	
	
	public boolean updateRecords(String strClusteringKeyValue,
			Hashtable<String,Object> htblColNameValue,String p,String key) throws Exception, IOException{
		
		boolean thisRecord=false;
		FileInputStream fi = new FileInputStream(p);
		ObjectInputStream objIn  = new ObjectInputStream(fi);
		Page thisPage=null;
		Object obj=null;
		int recordIndex=0;
		int ix=0;
		Record FinalRec=null;
		System.out.println(p);
		int indexToChange=0;//in record
		int indexOfRecord=0;//index of record in page

		try {
			while((obj = objIn.readObject()) != null) {
				if(obj instanceof Page) {
				Page page=(Page)obj;
				for(Record record:page.records) {//looping on records of page
					
		            for(int j=0;j<record.record.size() ;j++) { //looping on record		            	
		            	
		            	Object value = record.record.get(j);
		            	String stringValue = value.toString().trim(); // Convert to string and trim whitespaces

		            	
		            	if(htblColNameValue.containsKey(stringValue)) {
		            		System.out.println("possible update");
		            		System.out.println("htbl "+htblColNameValue.get(key));
		            		System.out.println("record: "+stringValue);
		            		
		            		indexToChange=(record.record.indexOf(value))+1;
		            		
		            		
		            	}
		            	
		            	
		            	if(stringValue.equals(strClusteringKeyColumn)) {
//		            		System.out.println("stringVal: "+stringValue);
//		            		System.out.println("clstrCol: "+strClusteringKeyColumn);
		            		String temp=record.record.get(j+1).toString().trim();
		            		if(temp.equals(strClusteringKeyValue)) {
			            		System.out.println("this is the record to be updated");
			            		System.out.println("temp: "+temp);
			            		System.out.println("clstr: "+strClusteringKeyValue);
			            		
			            		indexOfRecord=page.records.indexOf(record);
			            		thisPage=page;
			            		thisRecord=true;

		            		}
		            		
		            	}
		            }
		            	if(thisRecord==true) {
		            		

			            	//hott updated
			            	recordIndex=page.records.indexOf(record);
			            	ix=record.record.indexOf(key);
			            	Object deleteKeyFromBt=record.record.get(ix+1);
			            	
			            	if(isIndexed(key)) {
			            		
			            		for(String indexFilePath: index2) {
			            			updateIndex(indexFilePath, key, deleteKeyFromBt, page);
								
			    			}
			            	
		            	}
		            	
		            	
					}//end of reading records from page
					
					
					
				}//end of looping on page records
				
			}
				}//end of loop on pages
			}catch(EOFException e) {
				System.out.println(" End of reading file in update");
			}
			objIn.close();
			fi.close();
			
			
			if(thisRecord==true) {
				Record r=thisPage.records.get(indexOfRecord);
				r.record.set(indexToChange, htblColNameValue.get(key));
				
				
				try (FileOutputStream fileOutputStream = new FileOutputStream(p)) {
		            fileOutputStream.getChannel().truncate(0);
		        }
				
				//adding updated records
				FileOutputStream fileOut = new FileOutputStream(p);
		        ObjectOutputStream out = new ObjectOutputStream(fileOut);
		        
		        out.writeObject(thisPage);
		       
		        out.close();
		        fileOut.close();
				
				
			}
        		
		
			
			if(thisRecord==true)
				return true;
			else
				return false;
		
		}
	
	
	public void updateIndex(String indexFilePath,String key,Object deleteKeyFromBt,Page page)throws Exception {
		bplustree btII=null;
		Index i=null;
		FileInputStream fi = new FileInputStream(indexFilePath);
		ObjectInputStream objIn  = new ObjectInputStream(fi);
		Object obj1;
		String ifp="";
		try {
		while((obj1 = objIn.readObject()) != null) {
			if(obj1 instanceof Index) {
				 i=(Index)obj1;
				if(i.colName==key) {
					
					btII=i.bt;
					i.bt.delete(deleteKeyFromBt,(double) page.number);
					ifp=indexFilePath;
					}
				}
			}}catch(EOFException e) {
				System.out.println(" End of reading file in getTree");
			}
		objIn.close();
		fi.close();
		try (FileOutputStream fileOutputStream = new FileOutputStream(ifp)) {
            fileOutputStream.getChannel().truncate(0);
        }
		
		//adding updated records
		FileOutputStream fileOut = new FileOutputStream(ifp);
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
       
        out.writeObject(i);
        out.close();
        fileOut.close();
		
		
		
		
		}
		
		
	
	
	
	
	
	
	
	
	
	
	public void addToPage(String fileName,Record newTuple)throws Exception {
		
		
		Page page=null;
		//get page from memory(Deserialize)
		 FileInputStream fileIn = new FileInputStream(fileName);
		 ObjectInputStream objectIn  = new ObjectInputStream(fileIn);
		 Object obj;
			//add to page vector of records
		 try {
			while ((obj = objectIn.readObject()) != null) {
				if(obj instanceof Page) {
					page=(Page)obj;
					page.records.add(newTuple);
					
				}
				
			}
		 }catch(EOFException e) {
			 System.out.println("reading done in addToPage");
		 }
		//clearing the page content, to add it all over again 
		try (FileOutputStream fileOutputStream = new FileOutputStream(fileName)) {
            fileOutputStream.getChannel().truncate(0);
        }

		//serializing the page again
		FileOutputStream fileOut = new FileOutputStream(fileName);
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(page);
        
        //resource closing
        objectIn.close();
        fileIn.close();
        out.close();
        fileOut.close();
		 
	}


	public void deleteRecords(String p,Hashtable<String,Object> htblColNameValue,int conditions) throws Exception {

		FileInputStream fileIn = new FileInputStream(p);
		ObjectInputStream objectIn  = new ObjectInputStream(fileIn);
		Object obj;
		//reading from file vector by vector
		Vector<Record> toBeRemoved=new Vector<Record>();
		try {
		while ((obj = objectIn.readObject()) != null) {
			Boolean add=false;
			int passedConds=0;
			Page page=(Page)obj;
			toBeRemoved.removeAllElements();
			for(Record record:page.records) {
				passedConds=0;
				add=false;
				for(int j=0;j<record.record.size() && add==false ;j++) { // add==false ashan enta betlef as long as 
                	   //youre not sure if you should take it, once true stop and add it
					
                   	if(record.record.get(j) instanceof String) {
                   		if(htblColNameValue.containsKey((String)record.record.get(j))) {
                   			System.out.println("besmallah "+(String)record.record.get(j));
                   			System.out.println(record.record.toString());
                   			for(String s:htblColNameValue.keySet()) {
                   				System.out.println(htblColNameValue.get(s));
                   			}
                   			System.out.println("value from hashtabl of inputs "+htblColNameValue.get(record.record.get(j)));
                   			System.out.println("value from vector record "+record.record.get(j+1));
                   			if(htblColNameValue.get(record.record.get(j)).equals(record.record.get(j+1))) {
                   				System.out.println("val in final if "+record.record.get(j+1));
                   				//increment satisfied conds to check if all exist 
                   				passedConds++;
                   			}else {
                   				add=true;
                   			}
                   		}
                   	}
				}//end of looping on record
				//check if tuple should be removed
				if(passedConds==conditions) {
					toBeRemoved.add(record);
//					page.records.remove(record);//removing tuple from page vector
//					
//					//check if page is empty
//					if(page.records.isEmpty())
//						pageCount.remove(p);
//					else
//						pageCount.put(p, page.records.size());
//            	   //check if any index and remove
//            	   for(int j=0;j<record.record.size();j++) {
//            		   if(record.record.get(j) instanceof String ) {
//            			   String colName=((String)(record.record.get(j)));
//            			   if(htblColNameType.containsKey(colName)){
//            				   if(isIndexed(colName)) {
//            					   deleteFromTree(colName,record.record.get(j+1),page.number);
//                						 
//            			   }   
//            		   }   
//            	   } 
//                 }
            	   
				}//end if passedConds
				
			
			
			}//end of looping on page records
			
			
			for(Record record:toBeRemoved) {
				page.records.remove(record);//removing tuple from page vector
				
				//check if page is empty
				if(page.records.isEmpty())
					pageCount.remove(p);
				else
					pageCount.put(p, page.records.size());
        	   //check if any index and remove
        	   for(int j=0;j<record.record.size();j++) {
        		   if(record.record.get(j) instanceof String ) {
        			   String colName=((String)(record.record.get(j)));
        			   if(htblColNameType.containsKey(colName)){
        				   if(isIndexed(colName)) {
        					   deleteFromTree(colName,record.record.get(j+1),page.number);
            						 
        			   }   
        		   }   
        	   } 
             
			}
			
			
			
			//clearing the page content, to add it all over again 
			try (FileOutputStream fileOutputStream = new FileOutputStream(p)) {
	            fileOutputStream.getChannel().truncate(0);
	        } 
			//serialize page again
			FileOutputStream fileOut = new FileOutputStream(p);
	        ObjectOutputStream out = new ObjectOutputStream(fileOut);
	        out.writeObject(page);
	        
	        //resource closing
	        
	        out.close();
	        fileOut.close();
			
		}}}catch(EOFException e) {
			System.out.println(" End of reading file in delete");
		}
		objectIn.close();
        fileIn.close();
		
		
		
	}
	
	public void deleteFromTree(String colName,Object key, int PageNumber)throws Exception {
		bplustree bt=null;
		Index i=null;
		String ifp="";
		for(String indexFilePath: index2) {
			FileInputStream fileIn = new FileInputStream(indexFilePath);
			ObjectInputStream objectIn  = new ObjectInputStream(fileIn);
			Object obj;
			try {
			while((obj = objectIn.readObject()) != null) {
				if(obj instanceof Index) {
					 i=(Index)obj;
					if(i.colName==colName) {
						objectIn.close();
						fileIn.close();
						bt= i.bt;
						ifp=indexFilePath;
					}
				}
					
			}}catch(EOFException e) {
				System.out.println(" End of reading file in deleteFromTree");
			}
			objectIn.close();
			fileIn.close();
		}
		
		bt.delete(key,(double) PageNumber);
		try (FileOutputStream fileOutputStream = new FileOutputStream(ifp)) {
            fileOutputStream.getChannel().truncate(0);
        }
		
		//adding updated
		FileOutputStream fileOut = new FileOutputStream(ifp);
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        
        out.writeObject(i);
       
        out.close();
        fileOut.close();
		
		
	}
	
	public static void insertTableToCSV(String[] newTableInfo) {
        try {
            // Open the CSV file in append mode
            FileWriter fileWriter = new FileWriter("metadata.csv", true);
            CSVWriter csvWriter = new CSVWriter(fileWriter);

            // Write the new table information to the CSV file
            csvWriter.writeNext(newTableInfo);

            // Close the CSV writer
            csvWriter.close();
            fileWriter.close();

            System.out.println("Table inserted successfully at the end of the CSV file");

        } catch (IOException e) {
            e.printStackTrace();
        }
	}

	
	
	public static void rewriteCSVLine(String tableName, String columnName, String indexName) throws Exception {
	        // Open the CSV file for reading
	        CSVReader csvReader = new CSVReader(new FileReader("metadata.csv"));
	
	        // Read all records from the CSV file
	        List<String[]> allRecords = csvReader.readAll();
	        csvReader.close();
	
	        // Search for the line to rewrite based on the table name and column name
	        
	        for (String[] record : allRecords) {
	            if (record[0].equals(tableName) && record[1].equals(columnName)) {
	                // Rewrite the line with new values
	                record[4]=indexName;
	                record[5]="B+tree";
	                
	                break; // Stop searching after rewriting the line
	            }
	        }
	
	        // Open the CSV file for writing
	        CSVWriter csvWriter = new CSVWriter(new FileWriter("metadata.csv"));
	
	        // Write the updated records back to the CSV file
	        csvWriter.writeAll(allRecords);
	        csvWriter.close();
	    }
	
	//shahd
	


	public  ArrayList<Vector<Object>> addToEqual(Object value, Object myrecordvalue,Record record)throws Exception{
		 ArrayList<Vector<Object>> temp = new ArrayList<>();
		 try {
			 if (value instanceof Integer && myrecordvalue instanceof Integer) {
			        if (((Integer) value).equals((Integer) myrecordvalue)) {
			           temp.add(record.record);
			        }
			    } else if (value instanceof Double && myrecordvalue instanceof Double) {
			        if (Double.compare((Double) value, (Double) myrecordvalue) == 0) {
			          temp.add(record.record);
			        }
			    } else if (value instanceof String && myrecordvalue instanceof String) {
			        if (((String) value).equals((String) myrecordvalue)) {
			        	 temp.add(record.record);
			        }
			    }
			    else {
		            throw new IllegalArgumentException("Uncomparable types");
		        }
		 }
		 catch (Exception e) {
			 System.out.println("Exception occurred: " + e.getMessage());
		 
		 }
		 
			return temp; 
	}
	public  ArrayList<Vector<Object>> addToNotEqual(Object value, Object myrecordvalue,Record record)throws Exception{
		 ArrayList<Vector<Object>> temp = new ArrayList<>();
		 try {
			 if (value instanceof Integer && myrecordvalue instanceof Integer) {
			        if (!(((Integer) value).equals((Integer) myrecordvalue))) {
			           temp.add(record.record);
			        }
			    } else if (value instanceof Double && myrecordvalue instanceof Double) {
			        if (Double.compare((Double) value, (Double) myrecordvalue) != 0) {
			          temp.add(record.record);
			        }
			    } else if (value instanceof String && myrecordvalue instanceof String) {
			        if (!(((String) value).equals((String) myrecordvalue))) {
			        	 temp.add(record.record);
			        }
			    }
			    else {
		            throw new IllegalArgumentException("Uncomparable types");
		        }
		 }
		 catch (Exception e) {
			 System.out.println("Exception occurred: " + e.getMessage());
		 
		 }
		 
			return temp; 
	}
	public ArrayList<Vector<Object>> addToGreater(Object value, Object myrecordvalue, Record record)throws Exception {
	   ArrayList<Vector<Object>> temp = new ArrayList<>();
	   try {
	       if (value instanceof Integer && myrecordvalue instanceof Integer) {
	           if ((Integer) myrecordvalue > (Integer) value) {
	               temp.add(record.record);
	           }
	       } else if (value instanceof Double && myrecordvalue instanceof Double) {
	           if ((Double) myrecordvalue > (Double) value) {
	               temp.add(record.record);
	           }
	       } else if (value instanceof String && myrecordvalue instanceof String) {
	       	  if (((String) myrecordvalue).compareTo((String) value) > 0) {
	                 temp.add(record.record);
	             }
	       } else {
	           throw new IllegalArgumentException("Uncomparable types");
	       }
	   } catch (Exception e) {
	       System.out.println("Exception occurred: " + e.getMessage());
	   }

	   return temp;
	}
	public ArrayList<Vector<Object>> addToLess(Object value, Object myrecordvalue, Record record)throws Exception {
	   ArrayList<Vector<Object>> temp = new ArrayList<>();
	   try {
	       if (value instanceof Integer && myrecordvalue instanceof Integer) {
	           if ((Integer) myrecordvalue < (Integer) value) {
	               temp.add(record.record);
	           }
	       } else if (value instanceof Double && myrecordvalue instanceof Double) {
	           if ((Double) myrecordvalue < (Double) value) {
	               temp.add(record.record);
	           }
	       } else if (value instanceof String && myrecordvalue instanceof String) {
	       	  if (((String) myrecordvalue).compareTo((String) value) < 0) {
	                 temp.add(record.record);
	             }
	       } else {
	           throw new IllegalArgumentException("Uncomparable types");
	       }
	   } catch (Exception e) {
	       System.out.println("Exception occurred: " + e.getMessage());
	   }

	   return temp;
	}

			public  ArrayList<Vector<Object>> checkingIndexType(Object value,String tableName,String columnName,String operation,bplustree btree)throws Exception {
				 ArrayList<Vector<Object>> finalList = new ArrayList<>();
				//got pages numbers that holds value according to the operation
				try {
					ArrayList<Integer> pageKey=btree.getValues(value,operation);
					// for each page number will loop on its record
					for (int i=0;i<=pageKey.size();i++) {
						
						String pageName=tableName+""+pageKey.get(i)+".class";
						//Deserialize according to the pageName 
						FileInputStream fileIn = new FileInputStream(pageName);
						ObjectInputStream objectIn = new ObjectInputStream(fileIn);
						Object obj=null;
						//looping on records in the page till it's empty 
						try {
						 while ((obj = objectIn.readObject()) != null) {
							 if (obj instanceof Page) {
								 Page page = (Page) obj; 
								 if (page.number == pageKey.get(i)) {
									 for (Record record : page.records) {
										 	
										  Object myrecordvalue = new Vector<>(); 
										 for (int j = 0; j < record.record.size()-1; j++) {
											 if(record.record.get(j) instanceof String && ((String)record.record.get(j)).equals(columnName)) {
												 if(((String)record.record.get(j)).equals(columnName)) {
													 myrecordvalue=record.record.get(j+1);
												     if (operation.equals("=")) {
												    	 finalList.addAll(addToEqual(value,myrecordvalue,record));	
												     }
												     else if (operation.equals(">")) {
												    	 finalList.addAll(addToGreater(value,myrecordvalue,record));
												    			
												     }
												     else if (operation.equals("<")) {
												    	 finalList.addAll( addToLess(value,myrecordvalue,record));
												     }
												     else if (operation.equals("<=")) {
												    	 finalList.addAll( addToLess(value,myrecordvalue,record));
												    	 finalList.addAll(addToEqual(value,myrecordvalue,record));	
												     }
												     else if (operation.equals(">=")) {
												    	 finalList.addAll(addToGreater(value,myrecordvalue,record));
												    	 finalList.addAll(addToEqual(value,myrecordvalue,record));
												     }
												     else if (operation.equals("!=")){
												    	 finalList.addAll(addToNotEqual(value,myrecordvalue,record));
												     }
												     else {
												    	throw new  IllegalArgumentException("Invalid operation" );
												     }
												 }
											 }
										 }
									 }
								 }
							 
							 try (FileOutputStream fileOutputStream = new FileOutputStream(pageName)) {
						            fileOutputStream.getChannel().truncate(0);
						        } 
								//serialize page again
								FileOutputStream fileOut = new FileOutputStream(pageName);
						        ObjectOutputStream out = new ObjectOutputStream(fileOut);
						        out.writeObject(page);
						        
						        //resource closing
						        objectIn.close();
						        fileIn.close();
						        out.close();
						        fileOut.close();
							 }
						 }}catch(EOFException e) {
								System.out.println(" End of reading file in sql 1");
							}
				     }
				}
				catch (Exception e) {
					System.out.println("Exception occurred: " + e.getMessage());
				}
					return finalList;
				}
			
			public  ArrayList<Vector<Object>> checkingType(Object obj,Object value,String filename,String columnName,String operation)throws Exception {
				 ArrayList<Vector<Object>> finalList = new ArrayList<>();
				 try {
					 FileInputStream fileIn = new FileInputStream(filename);
			         ObjectInputStream objectIn  = new ObjectInputStream(fileIn);
			         try {
			         while ((obj = objectIn.readObject()) != null) {
			        	 if (obj instanceof Page) {
			        		 Page page=(Page)obj;
			        		 for ( Record record : page.records) {
								  Object myrecordvalue = new Vector<>(); 
								 for (int j = 0; j < record.record.size()-1; j++) {
									 if( record.record.get(j) instanceof String && ((String)  record.record.get(j)).equals(columnName)) {
										 if(((String) record.record.get(j)).equals(columnName)) {
											 myrecordvalue= record.record.get(j+1);
										     if (operation.equals("=")) {
										    	 finalList.addAll(addToEqual(value,myrecordvalue,record));	
										     }
										     else if (operation.equals(">")) {
										    	 finalList.addAll(addToGreater(value,myrecordvalue,record));
										    			
										     }
										     else if (operation.equals("<")) {
										    	 finalList.addAll( addToLess(value,myrecordvalue,record));
										     }
										     else if (operation.equals("<=")) {
										    	 finalList.addAll( addToLess(value,myrecordvalue,record));
										    	 finalList.addAll(addToEqual(value,myrecordvalue,record));	
										     }
										     else if (operation.equals(">=")) {
										    	 finalList.addAll(addToGreater(value,myrecordvalue,record));
										    	 finalList.addAll(addToEqual(value,myrecordvalue,record));
										     }
										     else if (operation.equals("!=")){
										    	 finalList.addAll(addToNotEqual(value,myrecordvalue,record));
										     }
										     else {
										    	throw new  IllegalArgumentException("Invalid operation" );
										     }
										 }
									 }
								 }
							 }
			        	 
			        	 try (FileOutputStream fileOutputStream = new FileOutputStream(filename)) {
					            fileOutputStream.getChannel().truncate(0);
					        } 
							//serialize page again
							FileOutputStream fileOut = new FileOutputStream(filename);
					        ObjectOutputStream out = new ObjectOutputStream(fileOut);
					        out.writeObject(page);
					        
					        //resource closing
					        objectIn.close();
					        fileIn.close();
					        out.close();
					        fileOut.close();
			        	 }
			         }}catch(EOFException e) {
							System.out.println(" End of reading file in sql 2");
						}
				 }
				 catch (Exception e) {
					 System.out.println("Exception occurred: " + e.getMessage());
				 }
				 return finalList;
			}
			public static boolean areVectorsEqual(Vector<Object> vector1, Vector<Object> vector2) {
		        // Check if the sizes are equal
		        if (vector1.size() != vector2.size()) {
		            return false;
		        }

		        // Iterate through each element and compare them
		        for (int i = 0; i < vector1.size(); i++) {
		            Object obj1 = vector1.get(i);
		            Object obj2 = vector2.get(i);

		            // Compare the current elements
		            if (obj1 == null) {
		                if (obj2 != null) {
		                    return false;
		                }
		            } else {
		                if (!obj1.equals(obj2)) {
		                    return false;
		                }
		            }
		        }

		        // All elements are equal
		        return true;
		    }
			public boolean isExist(ArrayList<Vector<Object>> temp,Vector<Object> myrecord) {
			boolean bool = false;
				for (int i=0;i<temp.size();i++) {
					if (areVectorsEqual(temp.get(i),myrecord)) {
						bool=true;
							break;
					}
				}
				return bool;
			}
			public boolean allisAnd(String[] strarrOperators) { 
				if (strarrOperators.length > 0) {
					for (int i=0;i<strarrOperators.length;i++) {
						if (!(strarrOperators[i].equals("AND"))) {
							return false;
						}
					}
					
				}
				return true;
			}
			public SQLTerm[] findIndex(SQLTerm[] arrSQLTerms) throws DBAppException {
				try {
				for (int i=0;i<arrSQLTerms.length;i++) {
					SQLTerm termf=arrSQLTerms[i];
					 String columnNamef = termf._strColumnName;
					 if(isIndexed(columnNamef)) {
						 SQLTerm temp=arrSQLTerms[0];
						 arrSQLTerms[0]=termf;
						 arrSQLTerms[i]=temp;
						 break;
						}
				}
				}
				catch(Exception e) {
					System.out.print("Exception occurred: " + e.getMessage());
				}
				return arrSQLTerms;
			}
	  public Iterator<?> selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
		
		if(allisAnd(strarrOperators)) {
			arrSQLTerms=findIndex(arrSQLTerms);
		}
		 ArrayList<Vector<Object>> FinalresultSet = new ArrayList<>();
		 ArrayList<Vector<Object>> temp = new ArrayList<>();
		 SQLTerm termf=arrSQLTerms[0];
		 String columnNamef = termf._strColumnName;
		 String tableNamef = termf._strTableName;
		 Object valuef = termf._objValue;
		String opf=termf._strOperator;
		try {
			if(isIndexed(columnNamef)) {
				bplustree btree=getTree(tableNamef);
				temp.addAll(checkingIndexType(valuef,tableNamef,columnNamef,opf,btree)); 
				//checkingIndexType(Object value,String tableName,String columnName,String operation,bplustree btree)
			}
			else {
				Table tablef=null;
				Iterator<Table> iterator=DBApp.tables.iterator();
				while(iterator.hasNext()) {
					Table t=iterator.next();
					String tName=t.tableName;
				if(tName.equals(tableNamef)){
						tablef=t;
						break;
				}	   
			}
				if (tablef != null) {	
					 for(String pageName:tablef.pageCount.keySet()) {
						
						 String filenamef=tablef.tableName+""+pages.size()+".class";
						 while(!(filenamef.equals(pageName))) {
							 filenamef=tablef.tableName+""+pages.size()+".class";
						 }
						 Object obj = null;
						 if (opf.equals("=")) {
								temp.addAll(checkingType(obj,valuef,filenamef,columnNamef,"="));
							}
						 else if(opf.equals("<")) {
								temp.addAll(checkingType(obj,valuef,filenamef,columnNamef,"<"));
							}
						 else if (opf.equals(">")) {
							 temp.addAll(checkingType(obj,valuef,filenamef,columnNamef,">"));
						 }
						 else if (opf.equals(">=")) {
							 temp.addAll(checkingType(obj,valuef,filenamef,columnNamef,">="));
						 }
						 else if (opf.equals("<=")) {
							 temp.addAll(checkingType(obj,valuef,filenamef,columnNamef,"<="));
						 }
						 else if (opf.equals("!=")) {
							 temp.addAll(checkingType(obj,valuef,filenamef,columnNamef,"!="));
						 }
						 else
								throw new IllegalArgumentException("Incorrect Operation");
					 }
				}
				
			}
			if(strarrOperators.length == 0) {
				FinalresultSet.addAll(temp);
			}
			else {
				for (int i = 0; i < strarrOperators.length; i++) {
					SQLTerm term1 = arrSQLTerms[i+1];
			        String columnName1 = term1._strColumnName;
			        String tableName1 = term1._strTableName;
			        Object value1 = term1._objValue;
			        String op1 = term1._strOperator;
			         if (columnName1.equals(columnNamef)) {   
			        	if (strarrOperators[i].equals("AND")) {
			        		if(isIndexed(columnName1)) {
								bplustree btree1=getTree(tableName1);
								temp.addAll(checkingIndexType(value1,tableName1,columnName1,op1,btree1)); 
							}
			        		else {
			        		 Iterator<Vector<Object>> iteratorAnd = temp.iterator();
			        		 while (iteratorAnd.hasNext()) {
			        			 Vector<Object> record = iteratorAnd.next();
						        	Object orgrecord1 = null;
						        	for (int z = 0; z < record.size() - 1; z++) {
						        		 if(record.get(z) instanceof String && ((String)record.get(z)).equals(columnName1)) {
						        			 orgrecord1 = record.get(z + 1); 
						        			 if (op1.equals("=")) {
						        			 if (value1 instanceof Integer && orgrecord1 instanceof Integer ) {
						        				 if((Integer)value1 != (Integer)orgrecord1) {
						        					 iteratorAnd.remove(); 
			                                            break;
						        				 } 
						        			 }
						        			 else if(value1 instanceof Double && orgrecord1 instanceof Double) {
						        				 if(((Double)value1) != ((Double)orgrecord1)) {
						        					 iteratorAnd.remove();
		                    						 break;
						        				 }
						        			 }
						        			 else if(value1 instanceof String && orgrecord1 instanceof String) {
						        				 if (value1.equals(orgrecord1)) {
						        					 iteratorAnd.remove();
	                   							 break;
						        				 }
						        			 }
						        			 else 
						        				 throw new IllegalArgumentException("Uncomparable types");
						        		 }
						        		 else if (op1.equals(">")) {
						        				if (value1 instanceof Integer && orgrecord1 instanceof Integer ) {
			                    					if(((Integer)value1) < ((Integer)orgrecord1) || ((Integer)value1) == ((Integer)orgrecord1)) {
			                    						iteratorAnd.remove();
		                    							 break;
			                    					}
			                    				}
						        				else if(value1 instanceof Double && orgrecord1 instanceof Double) {
			                    					if(((Double)value1) < ((Double)orgrecord1) || ((Double)value1) == ((Double)orgrecord1) ) {
			                    						iteratorAnd.remove();
		                    							 break;
			                    					}
			                    				}
						        				else if(value1 instanceof String && orgrecord1 instanceof String) {
						        					if(((String)orgrecord1).equals(value1) || (((String)orgrecord1).compareTo((String)value1)< 0 ) ) {
						        						iteratorAnd.remove();
		                    							 break;
						        					}
						        				}
						        				else 
		                    						throw new IllegalArgumentException("Uncomparable types");
						        		 }
						        		 else if (op1.equals("<")) {
						        			 if (value1 instanceof Integer && orgrecord1 instanceof Integer ) {
			                    					if(((Integer)value1) > ((Integer)orgrecord1) || ((Integer)value1)==((Integer)orgrecord1) ) {
			                    						iteratorAnd.remove();
		                    							 break;
			                    					}
			                    				}
						        			 else if(value1 instanceof Double && orgrecord1 instanceof Double) {
			                    					if(((Double)value1) > ((Double)orgrecord1) || ((Double)value1)==((Double)orgrecord1) ) {
			                    						iteratorAnd.remove();
		                    							 break;
			                    					}
			                    				}
						        			 else if(value1 instanceof String && orgrecord1 instanceof String) {
						        					if(((String)orgrecord1).equals(value1) || (((String)orgrecord1).compareTo((String)value1)> 0 ) ) {
						        						iteratorAnd.remove();
		                    							 break;
						        					}
						        				}
						        			 else 
						        				 throw new IllegalArgumentException("Uncomparable types");
						        		 }
						        		 else if (op1.equals("<=")) {
						        			 if (value1 instanceof Integer && orgrecord1 instanceof Integer ) {
			                    					if(((Integer)value1) > ((Integer)orgrecord1) ) {
			                    						iteratorAnd.remove();
		                    							 break;
			                    					}
			                    				}
						        			 else if(value1 instanceof Double && orgrecord1 instanceof Double) {
			                    					if(((Double)value1) > ((Double)orgrecord1)) {
			                    						iteratorAnd.remove();
		                    							 break;
			                    					}
			                    				}
						        			 else if(value1 instanceof String && orgrecord1 instanceof String) {
						        				 if((((String)orgrecord1).compareTo((String)value1)> 0 )) {
						        					 iteratorAnd.remove();
	                   							 break;
						        				 }
						        			 }
						        			 else 
						        				 throw new IllegalArgumentException("Uncomparable types");
						        		 	}
						        		 else if (op1.equals(">=")) {
						        			 if (value1 instanceof Integer && orgrecord1 instanceof Integer ) {
			                    					if(((Integer)value1) > ((Integer)orgrecord1)) {
			                    						iteratorAnd.remove();
		                    							 break;
			                    					}
			                    				}
						        			 else if(value1 instanceof Double && orgrecord1 instanceof Double) {
			                    					if(((Double)value1) > ((Double)orgrecord1) ) {
			                    						iteratorAnd.remove();
		                    							 break;
			                    					}
			                    				}
						        			 else if(value1 instanceof String && orgrecord1 instanceof String){
		                    						if((((String)orgrecord1).compareTo((String)value1) 	> 0 )) {
		                    							iteratorAnd.remove();
		                    							 break;
			                    					}
		                    					}
						        			 else 
						        					throw new IllegalArgumentException("Uncomparable types");
						        		 	}
						        		 else if (op1.equals("!=")) {
						        			 if (value1 instanceof Integer && orgrecord1 instanceof Integer ) {
			                    					if(((Integer)value1)== ((Integer)orgrecord1)) {
			                    						iteratorAnd.remove();
		                    							 break;
			                    					}
			                    				}
						        				else if(value1 instanceof Double && orgrecord1 instanceof Double) {
			                    					if(((Double)value1) == ((Double)orgrecord1)) {
			                    						iteratorAnd.remove();
		                    							 break;
			                    					}
			                    				}
						        				else if(value1 instanceof String && orgrecord1 instanceof String) {
						        					if(((String)orgrecord1).equals(value1)) {
						        						iteratorAnd.remove();
		                    							 break;
						        					}
						        				}
		                    					else 
		                    						throw new IllegalArgumentException("Uncomparable types");
						        		 		}
						        		 	 
						        		    else 
												throw new IllegalArgumentException("Incorrect Operation");
						        		 	}
						        		 }
						        	}
			        		   }
			        		 }
			        	else if (strarrOperators[i].equals("OR")) {
			        		 ArrayList<Vector<Object>> temp2 = new ArrayList<>();
			        		if(isIndexed(columnName1)) {
								bplustree btree1=getTree(tableName1); 
								temp2.addAll(checkingIndexType(value1,tableName1,columnName1,op1,btree1)); 
							}
			        		else {
			        			
								Table table1=null;
								Iterator<Table> iterator1=DBApp.tables.iterator();
								while(iterator1.hasNext()) {
									Table t1=iterator1.next();
									String tNamee=t1.tableName;
								if(tNamee.equals(tableName1)){
										table1=t1;
										break;
								}	   
							}
								if (table1 != null) {
									for(String pageName:table1.pageCount.keySet()) {
										String filename1=table1.tableName+""+pages.size()+".class";
										while(!(filename1.equals(pageName))) {
											filename1=table1.tableName+""+pages.size()+".class";
										}
										Object obj1 = null;
										 if (opf.equals("=")) {
												temp2.addAll(checkingType(obj1,value1,filename1,columnName1,"="));
											}
										 else if(opf.equals("<")) {
												temp2.addAll(checkingType(obj1,value1,filename1,columnName1,"<"));
											}
										 else if (opf.equals(">")) {
											 temp2.addAll(checkingType(obj1,value1,filename1,columnName1,">"));
										 }
										 else if (opf.equals(">=")) {
											 temp2.addAll(checkingType(obj1,value1,filename1,columnName1,">="));
										 }
										 else if (opf.equals("<=")) {
											 temp2.addAll(checkingType(obj1,value1,filename1,columnName1,"<="));
										 }
										 else if (opf.equals("!=")) {
											 temp2.addAll(checkingType(obj1,value1,filename1,columnName1,"!="));
										 }
										 else
											 throw new IllegalArgumentException("Incorrect Operation");
										 
									}
								}
			        		}
			        		if (temp2.size()>0) {
			        			for (int j=0;j<temp2.size();j++) {
			        				Vector<Object> value=temp2.get(j);
			        					if (!(isExist( temp,value))) {
			        						temp.add(value);
			        					}
			        			}
			        		}
			        	}
			        	else if (strarrOperators[i].equals("XOR")) {
			        		ArrayList<Vector<Object>> temp2 = new ArrayList<>();
							if (temp.size() == 0 ) {
								if(isIndexed(columnName1)) {
									bplustree btree1=getTree(columnName1);
									temp2.addAll(checkingIndexType(value1,tableName1,columnName1,op1,btree1)); 
								}
								else {
									Table table1=null;
									Iterator<Table> iterator1=DBApp.tables.iterator();
									while(iterator1.hasNext()) {
										Table t1=iterator1.next();
										String tNamee=t1.tableName;
										if(tNamee.equals(tableName1)){
											table1=t1;
											break;
									}	   
									}
									if (table1 != null) {
										for(String pageName:table1.pageCount.keySet()) {
											String filename1=table1.tableName+""+pages.size()+".class";
											while(!(filename1.equals(pageName))) {
												filename1=table1.tableName+""+pages.size()+".class";
											}
											Object obj1 = null;
											 if (opf.equals("=")) {
												 temp2.addAll(checkingType(obj1,value1,filename1,columnName1,"="));
											 }
											 else if(opf.equals("<")) {
													temp2.addAll(checkingType(obj1,value1,filename1,columnName1,"<"));
												}
											 else if(opf.equals(">")) {
													temp2.addAll(checkingType(obj1,value1,filename1,columnName1,">")); 		  
												}
												else if(opf.equals("<=")) {
													temp2.addAll(checkingType(obj1,value1,filename1,columnName1,"<="));
												}
												else if (opf.equals(">=")){
													temp2.addAll(checkingType(obj1,value1,filename1,columnName1,">="));
													}
												else if (opf.equals("!=")) {
													temp2.addAll(checkingType(obj1,value1,filename1,columnName1,"!="));
												    }
												else
													throw new IllegalArgumentException("Incorrect Operation");
										}
									}
								}
								if (temp2.size()> 0) {
									temp.addAll(temp2);
								}
								else {
									//not sure of Exception type 
									throw new ClassNotFoundException("Invalid");
								}
							}
								else {
									if(isIndexed(columnName1)) {
										bplustree btree1=getTree(tableName1);
										temp.addAll(checkingIndexType(value1,tableName1,columnName1,op1,btree1)); 
									}
									else {
									Iterator<Vector<Object>> iteratorXOR = temp.iterator();
					        		 while (iteratorXOR.hasNext()) {
					        			 Vector<Object> record = iteratorXOR.next();
								        	Object orgrecord1 = null;
								        	for (int z = 0; z < record.size() - 1; z++) {
								        		 if(record.get(z) instanceof String && ((String)record.get(z)).equals(columnName1)) {
								        			 orgrecord1 = record.get(z + 1); 
								        			 if (op1.equals("=")) {
								        			 if (value1 instanceof Integer && orgrecord1 instanceof Integer ) {
								        				 if((Integer)value1 != (Integer)orgrecord1) {
								        					 iteratorXOR.remove(); 
					                                            break;
								        				 } 
								        			 }
								        			 else if(value1 instanceof Double && orgrecord1 instanceof Double) {
								        				 if(((Double)value1) != ((Double)orgrecord1)) {
								        					 iteratorXOR.remove();
				                    						 break;
								        				 }
								        			 }
								        			 else if(value1 instanceof String && orgrecord1 instanceof String) {
								        				 if(((String)value1).equals((String)orgrecord1)) {
								        					 iteratorXOR.remove();
			                    							 break;
								        				 }
								        			 }
								        			 else 
								        				 throw new IllegalArgumentException("Uncomparable types");
								        		 }
								        		 else if (op1.equals(">=")) {
								        				if (value1 instanceof Integer && orgrecord1 instanceof Integer ) {
					                    					if(((Integer)value1) < ((Integer)orgrecord1) || ((Integer)value1) == ((Integer)orgrecord1)) {
					                    						iteratorXOR.remove();
				                    							 break;
					                    					}
					                    				}
								        				else if(value1 instanceof Double && orgrecord1 instanceof Double) {
					                    					if(((Double)value1) < ((Double)orgrecord1) || ((Double)value1) == ((Double)orgrecord1) ) {
					                    						iteratorXOR.remove();
				                    							 break;
					                    					}
					                    				}
								        				else if(value1 instanceof String && orgrecord1 instanceof String) {
								        					if(((String)orgrecord1).equals(value1) || (((String)orgrecord1).compareTo((String)value1)< 0 ) ) {
								        						iteratorXOR.remove();
				                    							 break;
								        					}
								        				}
								        				else 
				                    						throw new IllegalArgumentException("Uncomparable types");
								        		 }
								        		 else if (op1.equals("<=")) {
								        			 if (value1 instanceof Integer && orgrecord1 instanceof Integer ) {
					                    					if(((Integer)value1) > ((Integer)orgrecord1) || ((Integer)value1)==((Integer)orgrecord1) ) {
					                    						iteratorXOR.remove();
				                    							 break;
					                    					}
					                    				}
								        			 else if(value1 instanceof Double && orgrecord1 instanceof Double) {
					                    					if(((Double)value1) > ((Double)orgrecord1) || ((Double)value1)==((Double)orgrecord1) ) {
					                    						iteratorXOR.remove();
				                    							 break;
					                    					}
					                    				}
								        			 else if(value1 instanceof String && orgrecord1 instanceof String) {
								        					if(((String)orgrecord1).equals(value1) || (((String)orgrecord1).compareTo((String)value1)> 0 ) ) {
								        						iteratorXOR.remove();
				                    							 break;
								        					}
								        				}
								        			 else 
								        				 throw new IllegalArgumentException("Uncomparable types");
								        		 }
								        		 else if (op1.equals("<")) {
								        			 if (value1 instanceof Integer && orgrecord1 instanceof Integer ) {
					                    					if(((Integer)value1) > ((Integer)orgrecord1) ) {
					                    						iteratorXOR.remove();
				                    							 break;
					                    					}
					                    				}
								        			 else if(value1 instanceof Double && orgrecord1 instanceof Double) {
					                    					if(((Double)value1) > ((Double)orgrecord1)) {
					                    						iteratorXOR.remove();
				                    							 break;
					                    					}
					                    				}
								        			 else if(value1 instanceof String && orgrecord1 instanceof String) {
								        				 if((((String)orgrecord1).compareTo((String)value1)> 0 )) {
								        					 iteratorXOR.remove();
			                    							 break;
								        				 }
								        			 }
								        			 else 
								        				 throw new IllegalArgumentException("Uncomparable types");
								        		 	}
								        		 else if (op1.equals(">")) {
								        			 if (value1 instanceof Integer && orgrecord1 instanceof Integer ) {
					                    					if(((Integer)value1) < ((Integer)orgrecord1)) {
					                    						iteratorXOR.remove();
				                    							 break;
					                    					}
					                    				}
								        			 else if(value1 instanceof Double && orgrecord1 instanceof Double) {
					                    					if(((Double)value1) < ((Double)orgrecord1) ) {
					                    						iteratorXOR.remove();
				                    							 break;
					                    					}
					                    				}
								        			 else if(value1 instanceof String && orgrecord1 instanceof String){
				                    						if((((String)orgrecord1).compareTo((String)value1) 	< 0 )) {
				                    							iteratorXOR.remove();
				                    							 break;
					                    					}
				                    					}
								        			 else 
								        					throw new IllegalArgumentException("Uncomparable types");
								        		 	}
								        		 else if (op1.equals("!=")) {
								        			 if (value1 instanceof Integer && orgrecord1 instanceof Integer ) {
					                    					if(!(((Integer)value1)== ((Integer)orgrecord1))) {
					                    						iteratorXOR.remove();
				                    							 break;
					                    					}
					                    				}
								        				else if(value1 instanceof Double && orgrecord1 instanceof Double) {
					                    					if(!(((Double)value1) == ((Double)orgrecord1))) {
					                    						iteratorXOR.remove();
				                    							 break;
					                    					}
					                    				}
								        				else if(value1 instanceof String && orgrecord1 instanceof String) {
								        					if(!(((String)orgrecord1).equals(value1))) {
								        						iteratorXOR.remove();
				                    							 break;
								        					}
								        				}
				                    					else 
				                    						throw new IllegalArgumentException("Uncomparable types");
								        		 		}
								        		 	 
								        		    else 
														throw new IllegalArgumentException("Incorrect Operation");
								        		 	}
								        		 }
								        	}
									}
							}
			        	}
			        	else 
							throw new IllegalArgumentException("Incorrect Operation");
			        	}
			        else 
						throw new IllegalArgumentException("Incorrect Operation");
			        }
				   if (temp.size() == 0) {
			        	//not sure of Exception type 
						throw new IllegalArgumentException("Invalid");
			        }
				   else 
					   FinalresultSet.addAll(temp);
				}
			}
		
		catch(Exception e) {
			 throw new DBAppException("An error occurred during query execution: " + e.getMessage());
		}
		return FinalresultSet.iterator();

	}
	



	  public String getAllRecords() throws Exception{
		  String res="";
		  for(String p:pageCount.keySet()) {
				FileInputStream fileIn = new FileInputStream(p);
				ObjectInputStream objectIn  = new ObjectInputStream(fileIn);
				Object obj;
				//reading from file vector by vector
				try {
				while ((obj = objectIn.readObject()) != null) {
					if(obj instanceof Page) {
						Page page=(Page)obj;
						for(Record r:page.records) {
							res+=r.toString()+"\n";
							
						}
					}
				}}catch(EOFException e) {
					System.out.println("end of reading pages in get all records");
				}
		  }
		  return res;
	  }


}
	
	
	
	
	
	
	
	
	
	
	
		
	



