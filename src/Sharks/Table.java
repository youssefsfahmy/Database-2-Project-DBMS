package Sharks;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

public class Table implements Serializable{
	Vector<Page>Pages=new Vector(); //cannot be transient
	
	String TableName;
	String strClusteringKeyColumn;
	Hashtable<String,String> htblColNameType;
	
	Vector<String>  bTreeIndexedOn;
	Vector<Integer> bTreeIndexedOnCol;
	Vector<String>  rTreeIndexedOn;
	Vector<Integer> rTreeIndexedOnCol;
	
	
	public Table(String tableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType) throws IOException {
		TableName = tableName;
		this.strClusteringKeyColumn = strClusteringKeyColumn;
		this.htblColNameType = htblColNameType;
		this.bTreeIndexedOn= new Vector<String>();
		this.bTreeIndexedOnCol= new Vector<Integer>();
		this.rTreeIndexedOn = new Vector<String>();
		this.rTreeIndexedOnCol = new Vector<Integer>();
		
		this.saveTable();
		
		}
	
	public void saveTable() throws FileNotFoundException, IOException{
		
		ObjectOutputStream bin= new ObjectOutputStream( new FileOutputStream("data//"+ this.TableName +".class")); //creates new .class file for this page
		bin.writeObject(this); //serialize the vector to the .class file
		bin.flush();
		bin.close();
		
	}
	
	
	
	public void printTable() throws FileNotFoundException, ClassNotFoundException, IOException{
		System.out.println("Printing table: " + this.TableName);
		for(int i= 0;i<this.Pages.size();i++){
			Vector <Tuple> t = this.Pages.get(i).loadTuples();
			System.out.println("This is Page: " + this.Pages.get(i).pageName);
			for(int x=0; x<t.size();x++){
				System.out.println(t.get(x).toString());
				}
		}
	}
	
//	public Vector<Pages> getPages
	
	@SuppressWarnings("null")
	public Vector<String[]> getMetaData() throws IOException {
		Vector<String[]> metaData = new Vector();

		File f = new File("data\\metadata.csv");
		BufferedReader br = new BufferedReader(new FileReader(f));
		String data;

		while ((data = br.readLine()) != null) {
			String[] attributes = data.split(",");
			if (attributes[0].equals(this.TableName)) {
				metaData.add(attributes);

			}
		}

		return metaData;
	}
	
public void createBtreeIndex(String strColName, int colPosition) throws FileNotFoundException, IOException{

	if(!bTreeIndexedOn.contains(strColName)){
			this.bTreeIndexedOn.add(strColName);	
			this.bTreeIndexedOnCol.add(colPosition);

	}
	else{
		this.bTreeIndexedOn.remove(strColName);
		this.bTreeIndexedOnCol.remove((Object) colPosition);
		this.bTreeIndexedOn.add(strColName);	
		this.bTreeIndexedOnCol.add(colPosition);
	}

	this.saveTable();
}

public void createRtreeIndex(String strColName, int colPosition) throws FileNotFoundException, IOException{
	this.rTreeIndexedOn.add(strColName);
	this.rTreeIndexedOnCol.add(colPosition);
	this.saveTable();
}

public int findPageIndex(int pageNumber) {

	for(int i = 0;i<this.Pages.size();i++){
		if(this.Pages.get(i).number==pageNumber){
			return i;
		}
	}
	return -1;
}




	
	
	
	
}
