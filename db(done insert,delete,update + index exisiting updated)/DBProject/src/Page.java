import java.io.*;
import java.util.*;

public class Page implements Serializable {

	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1438473431006205025L;
	String name;
	int number;
	Vector<Record> records=new Vector<Record>();
	
	public Page(String name,int number) {
		this.name=name;
		this.number=number;
		
	}

	
	
	

}
