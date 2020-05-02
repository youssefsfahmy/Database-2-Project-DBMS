package Sharks;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Vector;

public class Test {
	

	public static void main(String[] args) throws IOException,
			ClassNotFoundException {
	
//		
//		String [] tuples= {"a","b","c"};
//		
//		
//		
//
//		
//			
//		
//		 ObjectOutputStream output = new ObjectOutputStream(
//		 new FileOutputStream("src//page1.bin"));
//		 output.writeObject(tuples);//rewrites the pages back
//		 output.flush();
//		 output.close();
//
//		 
//		Vector<TestPage> Table= new Vector();
//		
//		TestPage page1 = new TestPage();
//		
//		page1.name="page1";
//		page1.tuples=tuples;
//		Table.add(page1);
//		
//		
//		 ObjectOutputStream output1 = new ObjectOutputStream(
//		 new FileOutputStream("src//Table.bin"));
//		 output1.writeObject(Table);//rewrites the pages back
//		 output1.flush();
//		 output1.close();

//		ObjectInputStream input = new ObjectInputStream(new FileInputStream(
//				"src//Table.bin")); // get the table's file
//		Vector <TestPage> T = (Vector <TestPage>) input.readObject(); // deserialize the table's										// object
//
//		
//		
//		
//		ObjectInputStream input1 = new ObjectInputStream(new FileInputStream(
//				"src//"+T.get(0).name+".bin")); // get the table's file
//		String [] T1 = (String []) input1.readObject(); // deserialize the table's										// object
//		input.close();
//		T1[0]= "MARTHA";
//		
//
//		DBApp D = new DBApp();
//		Table T = D.loadTable("theTable");
//		T.printTable();

		
		
//		for(int i= 1;i<6;i++){
//			System.out.println("this is page " + i );
//			
//			ObjectInputStream input1 = new ObjectInputStream(new FileInputStream(
//					"src//sandour"+i+".bin")); // get the table's file
//			Vector <Tuple> t = (Vector <Tuple>) input1.readObject(); // deserialize the table's		
//			
//			for(int x=0; x<t.size();x++){
//				System.out.println(t.get(x).toString());
//				}		
//		}

//Object x = new Object();
//String y = "aaaa";
//x=(Object) y;
//String z = "aaaa";
//System.out.println(x.equals(z));

//System.out.println(T.get(0).toString());
		

	}
}