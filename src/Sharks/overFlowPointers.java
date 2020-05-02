package Sharks;
import java.io.Serializable;
import java.util.ArrayList;


public class overFlowPointers implements Serializable{
	public ArrayList<Pointer> values;
	
	public overFlowPointers(){
		values = new ArrayList<Pointer>();
	}
	public ArrayList<Pointer> getValues() {
		return values;
	}

	public void setValue(Object x) {
		this.values.add((Pointer)x);
	}
	
	public String toString(){
		return values.toString();
	}
	
	public void replace(Object x,Object y) throws DBAppException{
		
		Pointer Pvalue = (Pointer) x;
		int j = -1;
		for(int i = 0; i<this.values.size();i++){
			Pointer p1 = (Pointer) this.values.get(i);
			//System.out.println(p1.pageName+"    "+Pvalue.pageName);
			System.out.println("In Replace "+Pvalue.pageNumber+"   "+p1.pageNumber);
			if(p1.compareTo(Pvalue)==0){
				j=i;
				break;
			}
		}
		if(j==-1){
			throw new DBAppException("This pointer doesn't exist within this index");
		}
		else{
		values.set(j, (Pointer) y);
		}
	}
	
	public boolean isOverFlow(){
		if(this.values.size()==1){
			return false;
		}
		return true;
	}
	
	
	public static void main(String [] args){

	}
	
}
