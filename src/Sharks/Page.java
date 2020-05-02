package Sharks;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Vector;

public class Page implements java.io.Serializable {
	public int number;
	public transient Vector<Tuple> vectorArr;
	public String pageName;
	public Tuple firstKey;

	public Page(String name, Tuple t) throws FileNotFoundException,
			IOException, ClassNotFoundException {

		this.number = getPages("data//" + name + ".class"); // creates page number
															// using get pages
		this.pageName = name + number; // creates page name from table name and
										// page number

		this.vectorArr = new Vector<Tuple>(new DBApp().maxPage); // creates the
																	// tuple
																	// vector
																	// with the
																	// maxPage
																	// as its
																	// size
		this.vectorArr.add(t);
		this.firstKey = t;

		ObjectOutputStream bin = new ObjectOutputStream(new FileOutputStream(
				"data//" + name + number + ".class")); // creates new .class file for
													// this page
		bin.writeObject(this.vectorArr); // serialize the vector to the .class
											// file
		bin.flush();
		bin.close();

		ObjectInputStream input = new ObjectInputStream(new FileInputStream(
				"data//" + name + ".class")); // get the table's file
		Table T = (Table) input.readObject(); // deserialize the table's object
		input.close();
		T.Pages.add(this); // adds new page to the table's vector of pages

		ObjectOutputStream output = new ObjectOutputStream(
				new FileOutputStream("data//" + name + ".class"));
		output.writeObject(T);// rewrites the pages back
		output.flush();
		output.close();

	}

	public static int getPages(String dir) throws IOException,
			ClassNotFoundException {
		int num = 1; // number will be added as is!

		try {

			ObjectInputStream input = new ObjectInputStream(
					new FileInputStream(dir));
			Table T = (Table) input.readObject();
			input.close();
			if (T.Pages.size() == 0) {
				num = 1;
			} else {
				num = T.Pages.lastElement().number + 1;
			}

			ObjectOutputStream output = new ObjectOutputStream(
					new FileOutputStream(dir));
			output.writeObject(T);
			output.flush();
			output.close();

		} catch (Exception e) {

			e.printStackTrace();
		}

		return num;
	}

	public Vector<Tuple> loadTuples() throws FileNotFoundException,
			IOException, ClassNotFoundException {

		ObjectInputStream inputpage = new ObjectInputStream(
				new FileInputStream("data\\" + this.pageName + ".class")); // get
																		// the
																		// table's
																		// file
		Vector<Tuple> tuples = (Vector<Tuple>) inputpage.readObject(); // deserialize
																		// the
																		// table's
																		// object
		inputpage.close();

		this.firstKey = tuples.get(0);

		return tuples;
	}

	public void saveTuples(Vector<Tuple> tuples) throws IOException {

		this.firstKey = tuples.get(0);
		this.vectorArr = tuples;
		ObjectOutputStream output = new ObjectOutputStream(
				new FileOutputStream("data\\" + this.pageName + ".class"));
		output.writeObject(tuples);// rewrites the pages back
		output.flush();
		output.close();
	}

	public void removePage() {
		File file = new File("data\\" + this.pageName + ".class");

		if (file.delete()) {
			System.out.println("File deleted successfully");
		} else {
			System.out.println("Failed to delete the file");
		}

	}
}
