import java.io.Serializable;
import java.util.*;

public class Record implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 229754346971882197L;
	Vector<Object> record =new Vector<Object>();
	
	public Record(Hashtable<String,Object>  htblColNameValue) {
		
		for (String columnName : htblColNameValue.keySet()) {
            Object value =  htblColNameValue.get(columnName);
             //ashan akhliha fe format name,khaled,age,20,id,1
            
            record.add((Object)columnName);
            record.add(value);
        }
		
		
	}
	public Record(Vector<Object> data) {
		record=data;
		
	}
	
	@Override
	public String toString() {
	    
	    
	   String res="";
	    for (int i = 0; i < record.size();i+=2) {
	        res+=record.get(i+1)+" , ";;
	    
	     
	}
	return res;

}}
