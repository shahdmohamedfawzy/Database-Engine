import java.io.Serializable;

public class Index implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -856723443632654076L;
	String indexName;
	String colName;
	bplustree bt;
	public Index(String indexName,String colName) {
		this.indexName=indexName;
		this.colName=colName;
	}
	
	
}
