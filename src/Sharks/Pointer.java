package Sharks;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Vector;

public class Pointer implements Serializable, Comparable{

	String tableName;
	int pageNumber;
	int rowNumber;
	
	public Pointer(String tableName, int pageNumber, int rowNumber) {
		this.tableName = tableName;
		this.pageNumber = pageNumber;
		this.rowNumber = rowNumber;
	}
	
	public String toString(){
		return "(P:" + pageNumber + " R:" + rowNumber +")";
	}
	
	
	public Vector<Tuple> loadTuples() throws FileNotFoundException,
			IOException, ClassNotFoundException {

		ObjectInputStream inputpage = new ObjectInputStream(
				new FileInputStream("data\\" + this.tableName+this.pageNumber + ".class")); // get
																			// the
																			// table's
																			// file
		Vector<Tuple> tuples = (Vector<Tuple>) inputpage.readObject(); // deserialize
																		// the
																		// table's
																		// object
		inputpage.close();

		return tuples;
	}

	
	public Tuple loadTuple() throws FileNotFoundException, IOException,
			ClassNotFoundException {

		ObjectInputStream inputpage = new ObjectInputStream(
				new FileInputStream("data\\" + this.tableName + this.pageNumber
						+ ".class")); // get

		Vector<Tuple> tuples = (Vector<Tuple>) inputpage.readObject(); // deserialize
																		// the
																		// table's
																		// object
		inputpage.close();

		return tuples.get(this.rowNumber);
	}
	@Override
	public int compareTo(Object O) {
		Pointer P = (Pointer)O;
		int x = this.pageNumber - P.pageNumber;
		if(x!=0){
			return x;
		}
		return this.rowNumber - P.rowNumber;
	}
	
	public static ArrayList<Pointer> removeDuplicates(ArrayList<Pointer> list) 
    { 
  
        // Create a new ArrayList 
        ArrayList<Pointer> newList = new ArrayList<Pointer>(); 
  
        // Traverse through the first list 
        for (Pointer element : list) { 
  
            // If this element is not present in newList 
            // then add it 
            if (!newList.contains(element)) { 
  
                newList.add(element); 
            } 
        } 
  
        // return the new list 
        return newList; 
    } 
	// Overriding equals() to compare two Complex objects 
    @Override
    public boolean equals(Object o){
    	Pointer p = (Pointer) o;
    	
    	
    	if(this.pageNumber==p.pageNumber&&this.rowNumber==p.rowNumber){
    		return true;
    	}
    	return false;
    }

}
