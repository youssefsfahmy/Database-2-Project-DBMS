package Sharks;

import java.text.SimpleDateFormat;
import java.util.*;

import javax.print.attribute.IntegerSyntax;
import javax.swing.plaf.synth.SynthSeparatorUI;
import javax.swing.text.AbstractDocument.LeafElement;

import java.awt.Dimension;
import java.awt.Polygon;
import java.io.*;

public class DBApp {
	public int maxPage;
	public int nodeSize;
	// added this
	// Vector<Table> Tables = new Vector<Table>();

	// --------------ADDED THIS FUNCTION TO HANDLE POLYGONS----------------------
	public static String makeIntoString(int x[], int y[]) throws DBAppException {
		String result = "";
		if (x.length == y.length) {
			for (int i = 0; i < x.length; i++) {
				if (i == x.length - 1) {
					result = result + "(" + x[i] + "," + y[i] + ")";
				} else {
					result = result + "(" + x[i] + "," + y[i] + "),";
				}
			}
		} else {
			throw new DBAppException("Invalid INPUT");
		}
		return result;

	}

	// --------------------------------------------------------------
	// New
	public Vector<String> GetMyIndex(String tablename) throws IOException {
		Vector<String> myIndex = new Vector<String>();
		Vector<String[]> metadata = getMetaData(tablename);
		for (int i = 0; i < metadata.size(); i++) {
			if (metadata.get(i)[4].equals("true")) {
				myIndex.add(metadata.get(i)[1]);
				// System.out.println(metadata.get(i)[1]);
			}
		}

		return myIndex;
	}

	//update MS1
	public void updateTableO(String strTableName, String strKey, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, FileNotFoundException, IOException, Exception {

		Enumeration<String> enumeration = htblColNameValue.keys();
		String colname = "";
		Object value;
		int j = 0;
		Object[][] hash = new Object[htblColNameValue.size()][2];
		String keytype = "";
		Boolean replaced = false;

		Vector<String[]> metadata = getMetaData(strTableName);

		String keycol = getKey(metadata);
		ObjectInputStream input = new ObjectInputStream(new FileInputStream("data//" + strTableName + ".class")); // get
																													// the
																													// table's
																													// file
		Table T = (Table) input.readObject(); // deserialize the table's object
		input.close();

		// keytype
		boolean isPolygon = false;
		keytype = metadata.get(0)[2].toLowerCase();
		if (keytype.contains("polygon")) {
			isPolygon = true;
		}

		if (keytype.contains("date")) {

			String strClustringkey = strKey;

			String[] s = strClustringkey.split("-");

			Date d = new Date(Integer.parseInt(s[0]), Integer.parseInt(s[1]), Integer.parseInt(s[2]));

			strKey = d.toString();
		}
		while (enumeration.hasMoreElements()) {
			colname = enumeration.nextElement();
			value = htblColNameValue.get(colname);

			hash[j][0] = colname;
			hash[j][1] = value;

			j++;
		}

		for (int i = 1; i <= T.Pages.size(); i++) {

			ObjectInputStream inputpage = new ObjectInputStream(
					new FileInputStream("data\\" + T.TableName + i + ".class")); // get
																					// the
																					// table's
																					// file
			Vector<Tuple> tuples = (Vector<Tuple>) inputpage.readObject(); // deserialize
																			// the
																			// table's
																			// object
			inputpage.close();

			for (int y = 0; y < tuples.size(); y++) {

				String c = "";
				if (isPolygon) {
					int x[] = ((Polygon) ((tuples.get(y)).obj.get(0))).xpoints;
					int arr[] = ((Polygon) ((tuples.get(y)).obj.get(0))).ypoints;
					c = makeIntoString(x, arr);
				} else {
					c = ((tuples.get(y)).obj.get(0)).toString();
				}

				if (c.equalsIgnoreCase(strKey)) {
					replaced = true;
					tuples.get(y).obj.set(tuples.get(y).obj.size() - 1,
							new SimpleDateFormat("yyyy-MM-dd:HH-mm-ss").format(new Date()));

					for (int k = 0; k < hash.length; k++) {

						if (keycol.equalsIgnoreCase((String) hash[k][0])) {

							throw new DBAppException("please remove the clustering key from cols to be updated.");

						}
						String search = (String) hash[k][0];
						for (int m = 0; m < metadata.size(); m++) {
							if (metadata.get(m)[1].equalsIgnoreCase(search)) {
								String type = metadata.get(m)[2].toLowerCase();
								try {
									if (type.contains("integer")) {
										tuples.get(y).obj.set(m, (Integer) hash[k][1]);
									} else if (type.contains("double")) {
										tuples.get(y).obj.set(m, (Double) hash[k][1]);

									} else if (type.contains("string")) {
										tuples.get(y).obj.set(m, (String) hash[k][1]);

									} else if (type.contains("boolean")) {
										tuples.get(y).obj.set(m, (Boolean) hash[k][1]);

									} else if (type.contains("polygon")) {
										tuples.get(y).obj.set(m, (Polygon) hash[k][1]);

									} else if (type.contains("date")) {
										tuples.get(y).obj.set(m, (Date) hash[k][1]);

									}
								} catch (ClassCastException e) {
									System.out.println("You entered a wrong type");
								}
							}

						}
					}

				}

				ObjectOutputStream output = new ObjectOutputStream(
						new FileOutputStream("data\\" + T.TableName + i + ".class"));
				output.writeObject(tuples);// rewrites the pages back
				output.flush();
				output.close();

				// break;
			}

		}

		if (replaced == false) {
			throw new DBAppException("This key was never found");
		}

	}

	public void init() throws Exception {
		FileReader reader = new FileReader("config\\DBAPP.properties");
		Properties p = new Properties();
		p.load(reader);
		maxPage = Integer.parseInt(p.getProperty("MaximumRowsCountinPage"));
		nodeSize = Integer.parseInt(p.getProperty("NodeSize"));
	}

	//update MS1
	public void createTable00(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException, IOException {

		File tmpDir = new File("data\\" + strTableName + ".class");
		boolean exists = tmpDir.exists();

		if (!exists) {
			Table t = new Table(strTableName, strClusteringKeyColumn, htblColNameType);

		}

		else {
			throw new DBAppException("Table already existing");
			// return;
		}

		String colname = "";
		String coltype = "";
		Boolean key = false;
		Boolean indexed = false;
		Enumeration<String> enumeration = htblColNameType.keys();
		File f = new File("data\\metadata.csv");
		BufferedReader br = new BufferedReader(new FileReader(f));
		String data;
		String all = "";
		while ((data = br.readLine()) != null) {
			all += data + "\n";
		}

		BufferedWriter writer = new BufferedWriter(new FileWriter("data\\metadata.csv"));

		writer.write(all);

		while (enumeration.hasMoreElements()) {

			key = false;
			colname = enumeration.nextElement();
			coltype = htblColNameType.get(colname);

			if (strClusteringKeyColumn.equalsIgnoreCase(colname)) {
				key = true;

				String str = strTableName + "," + colname + "," + coltype + "," + key + "," + indexed + "\n";
				writer.write(str);
				break;
			}

		}
		if (!key) {
			throw new DBAppException("The clustering key is not equal any of the table contents");
		}

		Enumeration<String> enumeration1 = htblColNameType.keys();

		while (enumeration1.hasMoreElements()) {

			key = false;
			colname = enumeration1.nextElement();
			coltype = htblColNameType.get(colname);

			if (!(strClusteringKeyColumn.equalsIgnoreCase(colname))) {

				String str = strTableName + "," + colname + "," + coltype + "," + key + "," + indexed + "\n";
				writer.write(str);
			}

		}

		writer.close();

		System.out.println("The Table " + strTableName + "was created successfully");

		// added this only

	}

	
	//update MS2
	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException, IOException {

		File tmpDir = new File("data\\" + strTableName + ".class");
		boolean exists = tmpDir.exists();

		if (exists) {

			throw new DBAppException("Table already existing");

		}

		else {

			// return;

			String colname = "";
			String coltype = "";
			Boolean key = false;
			Boolean indexed = false;
			Enumeration<String> enumeration = htblColNameType.keys();
			File f = new File("data\\metadata.csv");
			BufferedReader br = new BufferedReader(new FileReader(f));
			String data;
			String all = "";
			while ((data = br.readLine()) != null) {
				all += data + "\n";
			}

			BufferedWriter writer = new BufferedWriter(new FileWriter("data\\metadata.csv"));

			writer.write(all);

			while (enumeration.hasMoreElements()) {

				key = false;
				colname = enumeration.nextElement();
				coltype = htblColNameType.get(colname);

				if (strClusteringKeyColumn.equalsIgnoreCase(colname)) {
					key = true;

					String str = strTableName + "," + colname + "," + coltype + "," + key + "," + indexed + "\n";
					writer.write(str);
					break;
				}

			}
			if (!key) {
				throw new DBAppException("The clustering key is not equal any of the table contents");
			}
			Table t = new Table(strTableName, strClusteringKeyColumn, htblColNameType);

			Enumeration<String> enumeration1 = htblColNameType.keys();

			while (enumeration1.hasMoreElements()) {

				key = false;
				colname = enumeration1.nextElement();
				coltype = htblColNameType.get(colname);

				if (!(strClusteringKeyColumn.equalsIgnoreCase(colname))) {

					String str = strTableName + "," + colname + "," + coltype + "," + key + "," + indexed + "\n";
					writer.write(str);
				}

			}

			writer.close();

			System.out.println("The Table " + strTableName + "was created successfully");

			// added this only
		}
	}

	// Load Rtree Added
	public static RTree loadRTree(String strTableName, String strColName) throws IOException, ClassNotFoundException {

		ObjectInputStream input = new ObjectInputStream(
				new FileInputStream("data\\" + "RTree" + strTableName + "On" + strColName + ".class")); // get the
																										// table's file
		RTree rt = (RTree) input.readObject(); // deserialize the table's object
		input.close();
		return rt;

	}

	// Load Btree
	public static BTree LoadBtree(String strTableName, String strColName) throws ClassNotFoundException, IOException {
		ObjectInputStream input = new ObjectInputStream(
				new FileInputStream("data\\" + "BTree" + strTableName + "On" + strColName + ".class")); // get the
																										// Btree's file
		BTree b = (BTree) input.readObject(); // deserialize the table's object
		input.close();
		return b;

	}

	public Table loadTable(String tablename) throws FileNotFoundException, IOException, ClassNotFoundException {

		ObjectInputStream input = new ObjectInputStream(new FileInputStream("data//" + tablename + ".class")); // get
																												// the
																												// table's
																												// file
		Table T = (Table) input.readObject(); // deserialize the table's object
		input.close();
		return T;
	}

	@SuppressWarnings("null")
	public Vector<String[]> getMetaData(String strTableName) throws IOException {
		Vector<String[]> metaData = new Vector();

		File f = new File("data\\metadata.csv");
		BufferedReader br = new BufferedReader(new FileReader(f));
		String data;

		while ((data = br.readLine()) != null) {
			String[] attributes = data.split(",");
			if (attributes[0].equals(strTableName)) {
				metaData.add(attributes);

			}
		}

		return metaData;
	}

	public String getKey(Vector<String[]> metadata) throws DBAppException {
		String key = "";
		try {
			for (int i = 0; i < metadata.size(); i++) {
				if (metadata.get(i)[3].equalsIgnoreCase("true")) {
					key = metadata.get(i)[1];

				}
			}
			return key;
		} catch (ArrayIndexOutOfBoundsException e) {
			// TODO: handle exception
			throw new DBAppException("Insert input correctly");
		}

	}

	/**
	 * These next methods are all related to insertion.
	 */

	// INSERT MILE STONE 2 INSERT (Playing here too)
	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, IOException, ClassNotFoundException {

		Vector<String[]> metadata = getMetaData(strTableName);

		Object[][] hash = new Object[htblColNameValue.size()][2];
		String key = getKey(metadata);
		if (metadata.size() == 0) {
			throw new DBAppException("error no such table");
			// return;
		}
		if (key.equals("")) {
			throw new DBAppException("NO KEY FOUND IN METADATA");
			// return;
		}

		Enumeration<String> enumeration = htblColNameValue.keys();
		String colname = "";
		Object value;

		int j = 0;
		while (enumeration.hasMoreElements()) {
			colname = enumeration.nextElement();
			value = htblColNameValue.get(colname);

			hash[j][0] = colname;
			hash[j][1] = value;

			j++;
		}

		Tuple T = new Tuple();
		for (int i = 0; i < metadata.size(); i++) {
			boolean found = false;

			for (int y = 0; y < hash.length; y++) {
				if (metadata.get(i)[1].equals(hash[y][0])) {
					found = true;
					String strColType = "class " + metadata.get(i)[2];
					Object strColValue = hash[y][1];

					if (strColType.equalsIgnoreCase(strColValue.getClass().toString())) {

						T.obj.add(strColValue);

					} else {
						throw new DBAppException("The insertion of (" + metadata.get(i)[1] + ") is not the right type");
						// return;
					}

				}
			}

			if (metadata.get(i)[1].equals(key)) {
				if (found == false) {

					throw new DBAppException("key not found in insertion");
					// return;
				}
			}

			if (found == false) {
				throw new DBAppException("The column " + metadata.get(i)[1] + " is not found in insertion");
				// return;
			}
		}

		T.obj.add(new SimpleDateFormat("yyyy-MM-dd:HH-mm-ss").format(new Date()));

		Table tb = this.loadTable(strTableName);

		if (tb.bTreeIndexedOn.contains(key)) {
			System.out.println("Adding this Record Using Btree on the Clustering Key");
			addTupleKT(strTableName, T, key, true);// boolean true represents BTREE s

		} else {

			if (tb.rTreeIndexedOn.contains(key)) {
				System.out.println("Adding this Record Using Rtree on the Clustering Key");
				addTupleKT(strTableName, T, key, false);// boolean false represents RTREE

			}

			else {
				addTuple(strTableName, T);
			}
		}

	}

//Binary Search In Update Martha
	static int binarySearch(Vector<Tuple> tuples, int l, int r, String strKey, String keytype) throws DBAppException {
		if (r >= l) {

			int mid = l + (r - l) / 2;
			System.out.println("Mid is" + mid);

			try {
				if (keytype.contains("integer")) {
					int d = Integer.parseInt(strKey);
					int d2 = (Integer) ((tuples.get(mid)).obj.get(0));
					if (d2 == d)
						return mid;

					if (d < d2) {
						System.out.println("left");
						return binarySearch(tuples, l, mid - 1, strKey, keytype);
					}

				} else if (keytype.contains("double")) {

					Double d = Double.parseDouble(strKey);
					Double d2 = (Double) ((tuples.get(mid)).obj.get(0));

					System.out.println("d is:" + d + " d2 is:" + d2);
					// If the element is present at the
					// middle itself
					if (d2.equals(d))
						return mid;

					// If element is smaller than mid, then
					// it can only be present in left subarray
					// System.out.println(" The c is "+c+" the clstrkey is: "+clstrkey);
					if (d < d2) {
						System.out.println("left");
						return binarySearch(tuples, l, mid - 1, strKey, keytype);
					}

				} else if (keytype.contains("boolean")) {
					Boolean d = Boolean.parseBoolean(strKey);
					Boolean d2 = (Boolean) ((tuples.get(mid)).obj.get(0));
					// If the element is present at the
// middle itself 
					if (d2.compareTo(d) == 0)
						return mid;

// If element is smaller than mid, then 
// it can only be present in left subarray 
// System.out.println(" The c is "+c+" the clstrkey is: "+clstrkey);
					if (d2.compareTo(d) > 0) {
						System.out.println("left");
						return binarySearch(tuples, l, mid - 1, strKey, keytype);
					}

				} else if (keytype.contains("polygon")) {
					Poly p1 = new Poly();

					Poly p = new Poly();

					Polygon d = makePolygon(strKey);// d
					p.p = d;
					p1.p = (Polygon) ((tuples.get(mid)).obj.get(0));
					// If the element is present at the
// middle itself 

					if (Arrays.equals(p1.p.xpoints, (d.xpoints)) && Arrays.equals(p1.p.ypoints, (d.ypoints)))
						return mid;

// If element is smaller than mid, then 
// it can only be present in left subarray 
// System.out.println(" The c is "+c+" the clstrkey is: "+clstrkey);
					if (p1.compareTo(p) >= 0) {
						System.out.println("left");
						return binarySearch(tuples, l, mid - 1, strKey, keytype);
					}

				} else if (keytype.contains("date")) {
					String strClustringkey = strKey;

					String[] s = strClustringkey.split("-");

					Date d = new Date(Integer.parseInt(s[0]), Integer.parseInt(s[1]), Integer.parseInt(s[2]));
					Date d2 = (Date) ((tuples.get(mid)).obj.get(0));
					System.out.println("The c is:" + d + "Thestrkey is" + d2 + "heee");

					// If the element is present at the
					// middle itself
					if (d2.compareTo(d) == 0)
						return mid;

					if (d2.compareTo(d) > 0) {
						System.out.println("left");
						return binarySearch(tuples, l, mid - 1, strKey, keytype);
					}

				} else {

					String d = strKey;
					String d2 = (String) ((tuples.get(mid)).obj.get(0));

					// If the element is present at the
					// middle itself
					if (d2.compareTo(d) == 0)
						return mid;

					// If element is smaller than mid, then
					// it can only be present in left subarray
					System.out.println(" The c is " + d2 + " the clstrkey is: " + d);
					if (d2.compareTo(d) > 0) {
						System.out.println("left");
						return binarySearch(tuples, l, mid - 1, strKey, keytype);
					}

				}
			} catch (Exception e) {
				throw new DBAppException("WRONG TYPE");

			}

			// Else the element can only be present
			// in right subarray
			System.out.println("right");

			return binarySearch(tuples, mid + 1, r, strKey, keytype);

		}

		// We reach here when element is not present
		// in array
		return -1;
	}

//INSERT BINARY SEARCH
	public int BinarysearchInsert(Vector<Tuple> t, Tuple t1) {
		int high = t.size() - 1;
		int low = 0;
		int pos = -1;
		// System.out.println("AYOOOOOO");

		if (t.isEmpty()) {
			pos = 0;
			// System.out.println("AYOOOOOO1");
			return pos;
		}

		while (low <= high) {
			int mid = (low + high) / 2;
			if (t.get(mid).compareTo(t1) < 0) {
				low = mid + 1;
				if (t.size() == low) {
					pos = t.size();
					return pos;
				}
				if (low > high) {// Found his location
					pos = low;
					return pos;
				}

			} else if (t.get(mid).compareTo(t1) > 0) {
				if (mid == 0) {// if i am the first element place me here
					pos = 0;
					return pos;
				}

				high = mid - 1;
				if (low > high) {
					pos = low;
					return pos;
				}
			}
			// when he finds an index zai ana momken at7t fel location di
			else if (t.get(mid).compareTo(t1) == 0) {

				if (t1.getClass().getName().contains("Polygon")) {
					while (mid >= 0) {

						if (t.get(mid).compareTo(t1) == 0) {
							mid--;

						} else {
							pos = mid;
							return pos;
						}

					}
				} else {

					pos = mid;
					return pos;
				}
			}

		}

		return pos;

	}

//INSERT New Check it (EDITED TO UPDATE WHILE INSERTING)
	public void InsertinAlltrees(Tuple Tuple, Pointer p, Table t)
			throws FileNotFoundException, ClassNotFoundException, IOException {

		Vector<String> Index = t.bTreeIndexedOn;
		Vector<Integer> colno = t.bTreeIndexedOnCol;

		for (int i = 0; i < Index.size(); i++) {

			System.out.println("inserted: " + Tuple.obj.get(colno.get(i)) + " in pointer: " + p);
			BTree b = LoadBtree(t.TableName, Index.get(i));
			updateShiftedPointers(b, p);
			b.insert((Comparable) Tuple.obj.get(colno.get(i)), p);
			b.saveBTree(t.TableName, Index.get(i));

		}

		Vector<String> IndexR = t.rTreeIndexedOn;
		Vector<Integer> colnoR = t.rTreeIndexedOnCol;

		for (int i = 0; i < IndexR.size(); i++) {

			RTree b = loadRTree(t.TableName, IndexR.get(i));
			updateShiftedPointers(b, p);
//		Poly p1 =new Poly();
//		p1.p=(Polygon)Tuple.obj.get(colno.get(i));
			Polygon polygon = (Polygon) Tuple.obj.get(colnoR.get(i));
			Poly poly = new Poly();
			poly.p = polygon;
			b.insert(poly, p);
			b.saveRTree(t.TableName, IndexR.get(i));

		}

	}

	// New for R tree Insert Updating Pointers

	public void updateShiftedPointers(RTree b, Pointer newp) {

		ArrayList<Pointer> pointerV = b.getLeafPointers();
		pointerV.sort(null);

		int nOP = maxPage;
		boolean done = false;
		pointerV.add(0, newp);
		pointerV.sort(null);

		int index = pointerV.indexOf(newp);

		if (index != 0) {
			if (pointerV.get(index).compareTo(pointerV.get(index - 1)) == 0) {
				pointerV.remove(index);
				pointerV.add(index - 1, newp);
			}
		}

		index = pointerV.indexOf(newp);

		if (pointerV.size() - 1 == index) {
			done = true;
			return;
		}

		for (int i = index + 1; i < pointerV.size() - 1; i++) {
			Pointer p = pointerV.get(i);
			Pointer pn = pointerV.get(i + 1);
			if ((p.pageNumber != newp.pageNumber) && i == index + 1) {
				done = true;
				return;
			}

			if (p.rowNumber == nOP - 1) {// if there is no space in page (last tuple)
				p.rowNumber = 0;
				p.pageNumber = pn.pageNumber;
			} else {// if there's space in the page

				System.out.println(pn + "" + p);
				if (pn.pageNumber != p.pageNumber) {// the next pointer is not within the page
					p.rowNumber = p.rowNumber + 1;
					done = true;
					System.out.println("AAAAlalalalalal");

					return;
				} else {
					p.rowNumber = p.rowNumber + 1;
				}
			}
		}
		if (!done) {
			Pointer p = pointerV.get(pointerV.size() - 1);
			if (p.rowNumber == nOP - 1) {
				p.rowNumber = 0;
				p.pageNumber = p.pageNumber + 1;
			} else {
				p.rowNumber = p.rowNumber + 1;
			}
		}

	}

	// INSERT New this is the new update method, shifts all pointers by 1
	public void updateShiftedPointers(BTree b, Pointer newp) {

		ArrayList<Pointer> pointerV = b.getLeafPointers();
		pointerV.sort(null);

		int nOP = maxPage;
		boolean done = false;
		pointerV.add(0, newp);
		pointerV.sort(null);

		int index = pointerV.indexOf(newp);

		if (index != 0) {
			if (pointerV.get(index).compareTo(pointerV.get(index - 1)) == 0) {
				pointerV.remove(index);
				pointerV.add(index - 1, newp);
			}
		}

		index = pointerV.indexOf(newp);

		if (pointerV.size() - 1 == index) {
			done = true;
			return;
		}

		for (int i = index + 1; i < pointerV.size() - 1; i++) {
			Pointer p = pointerV.get(i);
			Pointer pn = pointerV.get(i + 1);
			if ((p.pageNumber != newp.pageNumber) && i == index + 1) {
				done = true;
				return;
			}

			if (p.rowNumber == nOP - 1) {// if there is no space in page (last tuple)
				p.rowNumber = 0;
				p.pageNumber = pn.pageNumber;
			} else {// if there's space in the page

				System.out.println(pn + "" + p);
				if (pn.pageNumber != p.pageNumber) {// the next pointer is not within the page
					p.rowNumber = p.rowNumber + 1;
					done = true;
					System.out.println("AAAAlalalalalal");

					return;
				} else {
					p.rowNumber = p.rowNumber + 1;
				}
			}
		}
		if (!done) {
			Pointer p = pointerV.get(pointerV.size() - 1);
			if (p.rowNumber == nOP - 1) {
				p.rowNumber = 0;
				p.pageNumber = p.pageNumber + 1;
			} else {
				p.rowNumber = p.rowNumber + 1;
			}
		}

	}

	// INSERT MILE STONE 2
	public void addTuple(String tablename, Tuple t)
			throws FileNotFoundException, IOException, ClassNotFoundException, DBAppException {

		Table T = loadTable(tablename); // deserialize the table's object
		if (T.Pages.isEmpty()) {
			Page p = new Page(tablename, t);
			t.rowNumber = 0;
			t.pageNo = 1;

			InsertinAlltrees(t, new Pointer(tablename, 1, 0), T);// New Sandy

		} else {

			boolean inserted = false;

			for (int i = 0; i < T.Pages.size() - 1; i++) {

				Vector<Tuple> tuples = T.Pages.get(i).loadTuples();

				Tuple firstKey = T.Pages.get(i + 1).firstKey;

				if (tuples.size() < maxPage) {
					System.out.println("tuples.size() < maxPage");
					if (t.compareTo(tuples.lastElement()) < 0) {
						System.out.println("t.compareTo(tuples.lastElement()) < 0");
						int rowNo = insertIntoPage(t, tuples);
						t.pageNo = T.Pages.get(i).number;
						int page = T.Pages.get(i).number;
						System.out.println("f1");
						InsertinAlltrees(t, new Pointer(tablename, page, rowNo), T);// New Sandy

						inserted = true;
						// stopped here

						T.Pages.get(i).saveTuples(tuples);
						T.saveTable();
						break;

					} else {
						if (t.compareTo(firstKey) <= 0) {
							System.out.println("t.compareTo(firstKey) <= 0");
							int rowNo = insertIntoPage(t, tuples);
							t.pageNo = T.Pages.get(i).number;
							System.out.println("f2");
							InsertinAlltrees(t, new Pointer(tablename, t.pageNo, rowNo), T);// New Sandy

							inserted = true;

							T.Pages.get(i).saveTuples(tuples);
							T.saveTable();
							break;
						}

					}

				} else {
					if (t.compareTo(tuples.lastElement()) < 0) {

						System.out.println("insertion by shifting");

						Tuple shiftingT = tuples.lastElement();

						tuples.remove(maxPage - 1);

						int rowNo = insertIntoPage(t, tuples);
						t.pageNo = T.Pages.get(i).number;
						System.out.println("f3");
						InsertinAlltrees(t, new Pointer(tablename, t.pageNo, rowNo), T);// New Sandy

						inserted = true;

						T.Pages.get(i).saveTuples(tuples);
						T.saveTable();
						shiftT(i + 1, shiftingT, T);

						break;
					}
				}
			}

			if (!inserted) {

				Vector<Tuple> tuples = T.Pages.get(T.Pages.size() - 1).loadTuples();

				if (tuples.size() < maxPage) {

					int rowNo = insertIntoPage(t, tuples);

					t.pageNo = T.Pages.get(T.Pages.size() - 1).number;
					System.out.println("f4");
					InsertinAlltrees(t, new Pointer(T.TableName, t.pageNo, rowNo), T);// New Sandy

					T.Pages.get(T.Pages.size() - 1).saveTuples(tuples);
					T.saveTable();
				} else {
					if (t.compareTo(tuples.lastElement()) < 0) {

						Tuple shiftingT = tuples.lastElement();

						tuples.remove(maxPage - 1);

						int rowNo = insertIntoPage(t, tuples);
						t.pageNo = T.Pages.get(T.Pages.size() - 1).number;
						System.out.println("f5");
						InsertinAlltrees(t, new Pointer(T.TableName, t.pageNo, rowNo), T);// New Sandy

						inserted = true;

						T.Pages.get(T.Pages.size() - 1).saveTuples(tuples);
						T.saveTable();
						Page p = new Page(tablename, shiftingT);
						shiftingT.rowNumber = 0;
						shiftingT.pageNo = T.Pages.get(T.Pages.size() - 1).number;

//							updateAllBTrees(tablename, shiftingT, T.Pages.size()+1);//New Sandyyy

					} else {
						Page p = new Page(tablename, t);
						t.rowNumber = 0;
//							t.pageNo=T.Pages.size();//////NOT RIGHTTTT
						t.pageNo = p.number;
						InsertinAlltrees(t, new Pointer(tablename, t.pageNo, t.rowNumber), T);// New Sandy

					}

				}

			}
		}

	}

	// INSERT Adding the tuple if there exists a BTree on the clustering key
	// INSERT Adding the tuple if there exists a BTree on the clustering key
	public void addTupleKT(String strTableName, Tuple t, String keyCol, boolean b)
			throws ClassNotFoundException, IOException, DBAppException {

		Table T = this.loadTable(strTableName);

		if (T.Pages.isEmpty()) {
			Page p = new Page(strTableName, t);
			t.rowNumber = 0;
			t.pageNo = 1;

			InsertinAlltrees(t, new Pointer(strTableName, 1, 0), T);// New Sandy
			return;
		}

		Pointer p;
		if (b) {
			Comparable key = (Comparable) t.obj.get(0);
			BTree bt = this.LoadBtree(strTableName, keyCol);
			p = this.findPointerLoc(T, bt, key); // This returns the pointer so you could start adding the tuple in this
													// place ya sharkouu
		} else {
			Polygon polygon = (Polygon) t.obj.get(0);
			Poly poly = new Poly();
			poly.p = polygon;
			Poly key = poly;
			RTree rt = this.loadRTree(strTableName, keyCol);
			p = this.findPointerLoc(T, rt, key); // This returns the pointer so you could start adding the tuple in this
													// place ya sharkouu

		}


		if (p.pageNumber > (T.Pages.get(T.Pages.size() - 1)).number) {
			Page pa = new Page(strTableName, t);
			InsertinAlltrees(t, p, T);
			return;
		}

		int index = T.findPageIndex(p.pageNumber);

		boolean inserted = false;

		Vector<Tuple> tuples = T.Pages.get(index).loadTuples();

		if (tuples.size() < maxPage) {
			tuples.add(p.rowNumber, t);
			InsertinAlltrees(t, p, T);
			inserted = true;
			T.Pages.get(index).saveTuples(tuples);
			T.saveTable();
			return;

		} else {

			System.out.println("insertion by shifting");

			Tuple shiftingT = tuples.lastElement();

			tuples.remove(maxPage - 1);

			tuples.add(p.rowNumber, t);

			InsertinAlltrees(t, p, T);// New Sandy

			inserted = true;

			T.Pages.get(index).saveTuples(tuples);
			T.saveTable();
			shiftT(index + 1, shiftingT, T);

			return;
		}

		// PS: the pointer is not inserted in the tree after this method you still have
		// to use the InsertinAlltrees method while inserting

	}

	//Gets me The pointer to know where to insert incase of Btree
	public Pointer findPointerLoc(Table T, BTree bt, Comparable key) {
		overFlowPointers o = bt.search(key);

		if (o != null) {
			o.values.sort(null);

			return (Pointer) o.values.get(0);
		}

		Pointer p = new Pointer(T.TableName, -1, -1);

		bt.insert(key, p);

		// try 2

		ArrayList<Pointer> pointersV = bt.getLeafPointers();

		int shark = pointersV.indexOf(p);

		if (shark == 0) {
			p.pageNumber = T.Pages.get(0).number;
			;
			p.rowNumber = 0;
		} else {
			Pointer pb = pointersV.get(shark - 1);
			if (pb.rowNumber == maxPage - 1) {
				if (shark == pointersV.size() - 1) {
					p.pageNumber = pointersV.get(pointersV.size() - 2).pageNumber + 1;
					p.rowNumber = 0;

				} else {
					p.pageNumber = pointersV.get(shark + 1).pageNumber;
					p.rowNumber = 0;
				}
			} else {
				p.pageNumber = pb.pageNumber;
				p.rowNumber = pb.rowNumber + 1;
			}
		}
		System.out.println("SHARKK GAMDA W DA EL POINTER BTA3-HA" + p);

		if (1 == 1) {
			bt.delete(key);
			return p;
		}

		BTreeLeafNode ln = bt.findLeafNodeShouldContainKey(key);
		int kindex = -1;
		for (int i = 0; i < ln.keyCount; i++) {
			if (ln.keys[i].equals(key)) {
				kindex = i;
			}
		}

		if (kindex == -1) {
			System.out.println("ERRORRRR");
		}
		int pindex;

		if (kindex == 0) {
			if (ln.leftSibling == null) {
				p.pageNumber = 1;
				p.rowNumber = 0;
				bt.delete(key);
				return p;
			}
		}

		if (kindex == ln.keyCount - 1) {// if this is the last key in the leaf node
			if (ln.rightSibling == null) {// if there is no more leafs

				if (kindex == 0) {// if it is the first in the key (only 1 element)
					o = ((BTreeLeafNode) ln.leftSibling).values[((BTreeLeafNode) ln.leftSibling).values.length - 1];
					o.values.sort(null);
					p = o.values.get(o.values.size() - 1);
					bt.delete(key);

					if (p.rowNumber == maxPage - 1) {
						p.rowNumber = 0;
						p.pageNumber = p.pageNumber + 1;
					} else {
						p.rowNumber = p.rowNumber + 1;
					}

					return p;
				} else {

					o = ln.getValue(kindex - 1);
					o.values.sort(null);
					p = o.values.get(o.values.size() - 1);
					bt.delete(key);

					if (p.rowNumber == maxPage - 1) {
						p.rowNumber = 0;
						p.pageNumber = p.pageNumber + 1;
					} else {
						p.rowNumber = p.rowNumber + 1;
					}

					return p;

				}
			} else {
				o = ((BTreeLeafNode) ln.rightSibling).getValue(0);
				o.values.sort(null);
				p = o.values.get(0);
				bt.delete(key);
				return p;
			}
		} else {
			kindex = kindex + 1;
			o = ln.getValue(kindex);
			o.values.sort(null);
			p = o.values.get(0);
			bt.delete(key);
			return p;
		}

	}
	
	//Gets me The pointer to know where to insert incase of Rtree
	public Pointer findPointerLoc(Table T, RTree bt, Comparable key) {
		overFlowPointers o = bt.search(key);

		if (o != null) {
			o.values.sort(null);

			return (Pointer) o.values.get(0);
		}

		Pointer p = new Pointer(T.TableName, -1, -1);

		bt.insert(key, p);

		// try 2

		ArrayList<Pointer> pointersV = bt.getLeafPointers();

		int shark = pointersV.indexOf(p);

		if (shark == 0) {
			p.pageNumber = T.Pages.get(0).number;
			;
			p.rowNumber = 0;
		} else {
			Pointer pb = pointersV.get(shark - 1);
			if (pb.rowNumber == maxPage - 1) {
				if (shark == pointersV.size() - 1) {
					p.pageNumber = pointersV.get(pointersV.size() - 2).pageNumber + 1;
					p.rowNumber = 0;

				} else {
					p.pageNumber = pointersV.get(shark + 1).pageNumber;
					p.rowNumber = 0;
				}
			} else {
				p.pageNumber = pb.pageNumber;
				p.rowNumber = pb.rowNumber + 1;
			}
		}
		System.out.println("SHARKK GAMDA W DA EL POINTER BTA3-HA" + p);

		if (1 == 1) {
			return p;
		}

		BTreeLeafNode ln = bt.findLeafNodeShouldContainKey(key);
		int kindex = -1;
		for (int i = 0; i < ln.keyCount; i++) {
			if (ln.keys[i].equals(key)) {
				kindex = i;
			}
		}

		if (kindex == -1) {
			System.out.println("ERRORRRR");
		}
		int pindex;

		if (kindex == 0) {
			if (ln.leftSibling == null) {
				p.pageNumber = 1;
				p.rowNumber = 0;
				bt.delete(key);
				return p;
			}
		}

		if (kindex == ln.keyCount - 1) {// if this is the last key in the leaf node
			if (ln.rightSibling == null) {// if there is no more leafs

				if (kindex == 0) {// if it is the first in the key (only 1 element)
					o = ((BTreeLeafNode) ln.leftSibling).values[((BTreeLeafNode) ln.leftSibling).values.length - 1];
					o.values.sort(null);
					p = o.values.get(o.values.size() - 1);
					bt.delete(key);

					if (p.rowNumber == maxPage - 1) {
						p.rowNumber = 0;
						p.pageNumber = p.pageNumber + 1;
					} else {
						p.rowNumber = p.rowNumber + 1;
					}

					return p;
				} else {

					o = ln.getValue(kindex - 1);
					o.values.sort(null);
					p = o.values.get(o.values.size() - 1);
					bt.delete(key);

					if (p.rowNumber == maxPage - 1) {
						p.rowNumber = 0;
						p.pageNumber = p.pageNumber + 1;
					} else {
						p.rowNumber = p.rowNumber + 1;
					}

					return p;

				}
			} else {
				o = ((BTreeLeafNode) ln.rightSibling).getValue(0);
				o.values.sort(null);
				p = o.values.get(0);
				bt.delete(key);
				return p;
			}
		} else {
			kindex = kindex + 1;
			o = ln.getValue(kindex);
			o.values.sort(null);
			p = o.values.get(0);
			bt.delete(key);
			return p;
		}

	}

	
	// INSERT SHIFTING
	private void shiftT(int i, Tuple shiftingT, Table T)
			throws FileNotFoundException, IOException, ClassNotFoundException, DBAppException {

		if ((T.Pages.size()) == i) {
			Page p = new Page(T.TableName, shiftingT);
			shiftingT.rowNumber = 0;
			shiftingT.pageNo = p.number;
//				updateAllBTrees(T.TableName, shiftingT,T.Pages.size()+1);//New Sandy

		} else {

			Vector<Tuple> tuples = T.Pages.get(i).loadTuples();

			if (tuples.size() < maxPage) {
				insertIntoPage(shiftingT, tuples);
				shiftingT.pageNo = T.Pages.get(i).number;
//					updateAllBTrees(T.TableName, shiftingT,shiftingT.pageNo);//New Sandy

				T.Pages.get(i).saveTuples(tuples);
				T.saveTable();

			} else {
				Tuple nshiftingT = tuples.lastElement();

				tuples.remove(maxPage - 1);

				insertIntoPage(shiftingT, tuples);
				shiftingT.pageNo = T.Pages.get(i).number;
//					updateAllBTrees(T.TableName, shiftingT,shiftingT.pageNo);//New sandy

				T.Pages.get(i).saveTuples(tuples);
				T.saveTable();

				shiftT(i + 1, nshiftingT, T);

			}
		}

	}

	
	//Gets me the Location of the first Polygon
	private static int getmeMyPolyPos(Tuple t, Vector<Tuple> tuples, int pos) {
		int i;
		for (i = pos; i < tuples.size(); i++) {
			Polygon p1 = (Polygon) t.obj.get(0);
			Poly pp1 = new Poly();
			pp1.p = p1;

			Polygon p = (Polygon) tuples.get(pos).obj.get(0);
			Poly pp = new Poly();
			pp.p = p;

			if (pp.compareTo(pp1) == 0 && pp.compareCoordinates(pp1) == true) {

				return i;
			} else {
				if (pp.compareTo(pp1) == 0 && pp.compareCoordinates(pp1) == false) {

					pos++;
				} else {// law different area b2a
					return i;
				}

			}

		} // Ana 5last el page w mal2tsh el record aw different area //yeb2a Hatar aload w
			// adwar f page gedida :(
			// System.out.println(+ i );
		return i;

	}

	// INSERT MILESTONE 2 INSERT INTO PAGE
	private int insertIntoPage(Tuple t, Vector<Tuple> tuples) {
		System.out.println("Inserting using Binary Search");
		int pos = BinarysearchInsert(tuples, t);
		if (t.obj.get(0).getClass().getName().contains("Polygon")) {
			pos = getmeMyPolyPos(t, tuples, pos);
		}
		// System.out.println("Here 1 + Position Value is "+ pos);
		tuples.add(pos, t);
		t.rowNumber = pos;
		return pos;
		// System.out.println("Here 3");
		// tuples.add(t);
//
//	tuples.sort(null);

	}

	// OLDINSERT New Playing in This Insert (IF Key is CLustering bas lesa
	// mazbrhash)
	public void insertIntoTableO(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, IOException, ClassNotFoundException {

		Vector<String[]> metadata = getMetaData(strTableName);

		Object[][] hash = new Object[htblColNameValue.size()][2];
		String key = getKey(metadata);
		if (metadata.size() == 0) {
			throw new DBAppException("error no such table");
			// return;
		}
		if (key.equals("")) {
			throw new DBAppException("NO KEY FOUND IN METADATA");
			// return;
		}

		Enumeration<String> enumeration = htblColNameValue.keys();
		String colname = "";
		Object value;

		int j = 0;
		while (enumeration.hasMoreElements()) {
			colname = enumeration.nextElement();
			value = htblColNameValue.get(colname);

			hash[j][0] = colname;
			hash[j][1] = value;

			j++;
		}

		Tuple T = new Tuple();
		for (int i = 0; i < metadata.size(); i++) {
			boolean found = false;

			for (int y = 0; y < hash.length; y++) {
				if (metadata.get(i)[1].equals(hash[y][0])) {
					found = true;
					String strColType = "class " + metadata.get(i)[2];
					Object strColValue = hash[y][1];

					if (strColType.equalsIgnoreCase(strColValue.getClass().toString())) {

						T.obj.add(strColValue);

					} else {
						throw new DBAppException("The insertion of (" + metadata.get(i)[1] + ") is not the right type");
						// return;
					}

				}
			}

			if (metadata.get(i)[1].equals(key)) {
				if (found == false) {

					throw new DBAppException("key not found in insertion");
					// return;
				}
			}

			if (found == false) {
				throw new DBAppException("The column " + metadata.get(i)[1] + " is not found in insertion");
				// return;
			}
		}

		T.obj.add(new SimpleDateFormat("yyyy-MM-dd:HH-mm-ss").format(new Date()));

		// New Thought "Law Clustering Key Index Hatfkar ezayy"

//		Vector<String> Indexes = GetMyIndex(strTableName);
//		if(Indexes.contains(key)) {
//		//Hafakar had el Tuple ezay btr2a gedida 
//			addtuplethroughBtree(key,strTableName,T);
//			
//			
//		}
//		else {

		addTuple(strTableName, T);// ha insert el tuple 3adi bas
//		}
//what i need to manage here is to insert and update pointers in all Btree

	}

	// OLDINSERT the old insert into page
	private void insertIntoPageO(Tuple t, Vector<Tuple> tuples) {

		tuples.add(t);

		tuples.sort(null);

	}

	// OLDINSERT the old add tuple
	public void addTupleO(String tablename, Tuple t)
			throws FileNotFoundException, IOException, ClassNotFoundException, DBAppException {

		Table T = loadTable(tablename); // deserialize the table's object
		if (T.Pages.isEmpty()) {
			Page p = new Page(tablename, t);
			t.rowNumber = 0;
			t.pageNo = 1;
		} else {

			boolean inserted = false;

			for (int i = 0; i < T.Pages.size() - 1; i++) {

				Vector<Tuple> tuples = T.Pages.get(i).loadTuples();

				Tuple firstKey = T.Pages.get(i + 1).firstKey;

				if (tuples.size() < maxPage) {
					if (t.compareTo(tuples.lastElement()) < 0) {

						insertIntoPage(t, tuples);
						t.pageNo = i + 1;

						inserted = true;
						// stopped here

						T.Pages.get(i).saveTuples(tuples);
						T.saveTable();
						break;

					} else {
						if (t.compareTo(firstKey) <= 0) {
							insertIntoPage(t, tuples);
							t.pageNo = i + 1;

							inserted = true;

							T.Pages.get(i).saveTuples(tuples);
							T.saveTable();
							break;
						}

					}

				} else {
					if (t.compareTo(tuples.lastElement()) < 0) {

						System.out.println("insertion by shifting");

						Tuple shiftingT = tuples.lastElement();

						tuples.remove(maxPage - 1);

						insertIntoPage(t, tuples);
						t.pageNo = i + 1;

						inserted = true;

						T.Pages.get(i).saveTuples(tuples);
						T.saveTable();
						shiftT(i + 1, shiftingT, T);

						break;
					}

				}

				// if (tuples.size() < maxPage) {
				//
				// System.out.println("insertion in page "
				// + T.Pages.get(i).pageName
				// + " because i found an empty page");
				//
				// insertIntoPage(t, tuples);
				//
				// inserted = true;
				//
				// T.Pages.get(i).saveTuples(tuples);
				//
				// break;
				//
				// } else {
				//
				// if (t.compareTo(tuples.lastElement()) < 0) {
				//
				// System.out.println("insertion by shifting");
				//
				// Tuple shiftingT = tuples.lastElement();
				//
				// tuples.remove(maxPage - 1);
				//
				// insertIntoPage(t, tuples);
				//
				// inserted = true;
				//
				// T.Pages.get(i).saveTuples(tuples);
				//
				// shiftT(i + 1, shiftingT, T);
				//
				// break;
				// }
				// }
			}

			if (!inserted) {

				Vector<Tuple> tuples = T.Pages.get(T.Pages.size() - 1).loadTuples();

				if (tuples.size() < maxPage) {
					insertIntoPage(t, tuples);
					t.pageNo = T.Pages.get(T.Pages.size() - 1).number;
					T.Pages.get(T.Pages.size() - 1).saveTuples(tuples);
					T.saveTable();
				} else {
					if (t.compareTo(tuples.lastElement()) < 0) {

						Tuple shiftingT = tuples.lastElement();

						tuples.remove(maxPage - 1);

						insertIntoPage(t, tuples);
						t.pageNo = T.Pages.get(T.Pages.size() - 1).number;

						inserted = true;

						T.Pages.get(T.Pages.size() - 1).saveTuples(tuples);
						T.saveTable();
						Page p = new Page(tablename, shiftingT);
						shiftingT.rowNumber = 0;
						shiftingT.pageNo = T.Pages.get(T.Pages.size() - 1).number;

					} else {
						Page p = new Page(tablename, t);
						t.rowNumber = 0;
						t.pageNo = T.Pages.get(T.Pages.size() - 1).number;

					}

				}

			}
		}

	}

	// MILESTONE 2 DELETE METHOD
	// MILESTONE 2 DELETE METHOD
	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, IOException, ClassNotFoundException {

		Vector<String[]> metadata = getMetaData(strTableName);

		Object[][] hash = new Object[htblColNameValue.size()][5];// col0 = colname, col1= value, col2 = position in
																	// tuple, col3 = indexed, col4 = is polygon
		if (hash.length == 0) {
			throw new DBAppException("No delete criteria");

		}
		if (metadata.size() == 0) {
			throw new DBAppException("error no such table");
			// return;
		}

		Enumeration<String> enumeration = htblColNameValue.keys();
		String colname = "";
		Object value;
		boolean isPolygon;

		int j = 0;

		while (enumeration.hasMoreElements()) {
			colname = enumeration.nextElement();
			value = htblColNameValue.get(colname);
			isPolygon = false;
			hash[j][0] = colname;
			hash[j][1] = value;
			boolean found = false;
			for (int x = 0; x < metadata.size(); x++) {

				if (metadata.get(x)[1].equalsIgnoreCase(colname)) {
					found = true;

					String strColType = "class " + metadata.get(x)[2];
					Object strColValue = hash[j][1];
					hash[j][3] = metadata.get(x)[4];

					if (strColType.equalsIgnoreCase(strColValue.getClass().toString())) {
						hash[j][4] = false;
						if (strColType.contains("Polygon")) {
							isPolygon = true;// is polygon
						}
						hash[j][4] = isPolygon;
						hash[j][2] = x;

						break;

					} else {
						throw new DBAppException("The insertion of (" + metadata.get(x)[1] + ") is not the right type");
						// return;
					}

				}
			}
			if (!found) {
				throw new DBAppException("The insertion col name does not exist");

			}
			j++;
		}

		Table T = loadTable(strTableName);

		String keyName = getKey(getMetaData(strTableName));
		boolean found2 = false;
		boolean indexed = false;
		Object keyValue = null;
		for (int i = 0; i < hash.length; i++) {
			if (((String) hash[i][3]).equalsIgnoreCase("true")) {
				indexed = true;

			}

			boolean keyPoly = false;
			String colname2 = hash[i][0].toString();
			if (keyName.equals(colname2)) {
				found2 = true;
				keyValue = hash[i][1];
			}
		}

		int PagetupleIn = -1;

		if (indexed == false) {

			System.out.println("indexed is " + indexed + " the colName " + hash[0][0]);
			if (found2) {
				System.out.println("Using Binary Search");

				Tuple t1 = new Tuple();

				t1.obj.add(keyValue);

				Vector<Page> Pages = T.Pages;

				for (int i = 0; i < Pages.size() - 1; i++) {
					Tuple firstElementNextPage = T.Pages.get(i + 1).firstKey;
					int comp2 = firstElementNextPage.compareTo(t1);
					if (comp2 >= 0) {

						PagetupleIn = i;
						break;
					}
				}

				if (PagetupleIn == -1) {
					PagetupleIn = Pages.size() - 1;
				}

				Vector<Tuple> tuples = T.Pages.get(PagetupleIn).loadTuples();

				int tupleIndex = Collections.binarySearch(tuples, t1);
				for (int k = tupleIndex; k >= 0; k--) {// tuple
					Tuple t2 = tuples.get(k);

					int compare = t1.compareTo(t2);
					System.out.println(k);
					if (compare != 0) {
						tupleIndex = k + 1;
						break;
					}
					if (k == 0) {
						tupleIndex = 0;
						break;
					}
				}
				if (tupleIndex < 0) {
					tupleIndex = 0;
					PagetupleIn = PagetupleIn + 1;
					tuples = T.Pages.get(PagetupleIn).loadTuples();

				}
				System.out.println("page index tuple in" + tupleIndex);

				System.out.println("page tuple in" + PagetupleIn);

				boolean diffElementFound = false;
				boolean firstEntryToLoop = true;

				for (int i = PagetupleIn; i < T.Pages.size(); i++) {// pages

					if (firstEntryToLoop) {

						firstEntryToLoop = false;

					} else {

						tuples = T.Pages.get(i).loadTuples();
					}

					for (int k = tupleIndex; k < tuples.size(); k++) {// tuple
						Vector<Object> t = tuples.get(k).obj;
						Tuple t2 = tuples.get(k);
						boolean delete = true;

//Stopped here 
						for (int z = 0; z < hash.length; z++) {
							int compare = t1.compareTo(t2);

							if (compare != 0) {
								diffElementFound = true;
							}

							if ((boolean) hash[z][4] == true) {// Is a Polygon
								Polygon dummyPol = (Polygon) hash[z][1];
								Polygon toComparewth = (Polygon) t.get((int) hash[z][2]);

								Poly dummyPoly = new Poly();
								dummyPoly.p = dummyPol;

								Poly toComparewthPoly = new Poly();
								toComparewthPoly.p = toComparewth;

//							Arrays.sort(dummyPol.xpoints);
//							Arrays.sort(dummyPol.ypoints);
//							Arrays.sort(toComparewth.xpoints);
//							Arrays.sort(toComparewth.ypoints);

								if (!(dummyPoly.compareCoordinates((toComparewthPoly)))) {
									delete = false;
								}

							} else {
								if (!(hash[z][1].equals(t.get((int) hash[z][2])))) {
									delete = false;
								}
							}
						}

						if (delete) {
							System.out.println("Deleted one record");
							tuples.remove(k);
							deleteinAllBtrees(t2, new Pointer(strTableName, t2.pageNo, k), T);
							k--;
						}
					}
					tupleIndex = 0;

					if (tuples.size() == 0) {
						T.Pages.get(i).removePage();
						T.Pages.remove(i);
						T.saveTable();
						i = i - 1;
					} else {
						T.Pages.get(i).saveTuples(tuples);
						T.saveTable();
					}
					if (diffElementFound) {
						break;
					}

				}
			}

			else {

				for (int i = 0; i < T.Pages.size(); i++) {// pages
					Vector<Tuple> tuples = T.Pages.get(i).loadTuples();
					for (int k = 0; k < tuples.size(); k++) {// tuple
						Vector<Object> t = tuples.get(k).obj;

						boolean delete = true;

						for (int z = 0; z < hash.length; z++) {// hash
							// Trying here

							if ((boolean) hash[z][4] == true) {// Is a Polygon
								Polygon dummyPol = (Polygon) hash[z][1];
								Polygon toComparewth = (Polygon) t.get((int) hash[z][2]);

								Poly dummyPoly = new Poly();
								dummyPoly.p = dummyPol;

								Poly toComparewthPoly = new Poly();
								toComparewthPoly.p = toComparewth;

//						Arrays.sort(dummyPol.xpoints);
//						Arrays.sort(dummyPol.ypoints);
//						Arrays.sort(toComparewth.xpoints);
//						Arrays.sort(toComparewth.ypoints);
								System.out.println(
										"the polygons are " + makeIntoString(dummyPol.xpoints, dummyPol.ypoints)
												+ " AND " + makeIntoString(toComparewth.xpoints, toComparewth.ypoints));

								if (!(dummyPoly.compareCoordinates((toComparewthPoly)))) {
									delete = false;
								}

							} else {
								if (!(hash[z][1].equals(t.get((int) hash[z][2])))) {
									delete = false;
								}
							}

//					if (!(hash[z][1].equals(t.get((int) hash[z][2])))) {
//						delete = false;
//					}
						}

						if (delete) {
							System.out.println("Deleted one record");
							Tuple variable = tuples.remove(k);
							deleteinAllBtrees(variable, new Pointer(strTableName, variable.pageNo, k), T);
							k--;
						}
					}

					if (tuples.size() == 0) {
						T.Pages.get(i).removePage();
						T.Pages.remove(i);
						T.saveTable();
						i = i - 1;
					} else {
						T.Pages.get(i).saveTuples(tuples);
						T.saveTable();
					}

				}

			}

			///////////////////////// habal

		}

		else {
			this.deleteUsingIndex(T, hash);
		}
	}

	private void deleteUsingIndex(Table T, Object[][] hash) throws ClassNotFoundException, IOException, DBAppException {

		ArrayList<Pointer> pointerV = new ArrayList<Pointer>();
		boolean first = true;
		for (int i = 0; i < hash.length; i++) {

			if ((((String) hash[i][3]).equalsIgnoreCase("true")) && ((boolean) hash[i][4]) == true) {// checks
																										// to
																										// see
																										// if
																										// RTree
				RTree bt = this.loadRTree(T.TableName, (String) hash[i][0]);
				Polygon polygon = (Polygon) hash[i][1];
				Poly poly = new Poly();
				poly.p = polygon;
				overFlowPointers o = bt.search(poly);
				if (o == null) {
					return;
				}

				if (first) {
					for (int j = 0; j < o.values.size(); j++) {
						pointerV.add(o.values.get(j));
						first = false;
					}
				} else {
					ArrayList<Pointer> pointerVn = new ArrayList<Pointer>();
					for (int j = 0; j < o.values.size(); j++) {
						pointerVn.add(o.values.get(j));
					}
					pointerV = this.andingPointers(pointerV, pointerVn);
				}

			} else {
				if (((String) hash[i][3]).equalsIgnoreCase("true")) {
					BTree bt = this.LoadBtree(T.TableName, (String) hash[i][0]);
					overFlowPointers o = bt.search((Comparable) hash[i][1]);
					if (o == null) {
						return;
					}

					if (first) {
						for (int j = 0; j < o.values.size(); j++) {
							pointerV.add(o.values.get(j));
							first = false;
						}
					} else {
						ArrayList<Pointer> pointerVn = new ArrayList<Pointer>();
						for (int j = 0; j < o.values.size(); j++) {
							pointerVn.add(o.values.get(j));
						}
						pointerV = this.andingPointers(pointerV, pointerVn);
					}
				}
			}
		}

		pointerV.sort(null);
		System.out.println(pointerV);
		int i = 0;
		while (i < pointerV.size()) {
			int pageno = pointerV.get(i).pageNumber;
			Vector<Tuple> tuples = pointerV.get(i).loadTuples();
			ArrayList<Pointer> pointerVpo = new ArrayList<Pointer>();

			while (i < pointerV.size() && pointerV.get(i).pageNumber == pageno) {
				pointerVpo.add(pointerV.get(i));
				i++;
			}
			ArrayList<Pointer> pointerVp = new ArrayList<Pointer>();
			while (pointerVpo.size() > 0) {
				pointerVp.add(pointerVpo.remove(pointerVpo.size() - 1));
			}

			int j = 0;
			System.out.println(pointerVp);
			while (j < pointerVp.size()) {
				int rowno = pointerVp.get(j).rowNumber;
				System.out.println(rowno);
				Vector<Object> t = tuples.get(rowno).obj;

				boolean delete = true;

				for (int z = 0; z < hash.length; z++) {// hash
					if ((boolean) hash[z][4] == true) {// Is a Polygon
						Polygon dummyPol = (Polygon) hash[z][1];
						Polygon toComparewth = (Polygon) t.get((int) hash[z][2]);

						Poly dummyPoly = new Poly();
						dummyPoly.p = dummyPol;

						Poly toComparewthPoly = new Poly();
						toComparewthPoly.p = toComparewth;

//						Arrays.sort(dummyPol.xpoints);
//						Arrays.sort(dummyPol.ypoints);
//						Arrays.sort(toComparewth.xpoints);
//						Arrays.sort(toComparewth.ypoints);
						System.out.println("the polygons are " + makeIntoString(dummyPol.xpoints, dummyPol.ypoints)
								+ " AND " + makeIntoString(toComparewth.xpoints, toComparewth.ypoints));

						if (!(dummyPoly.compareCoordinates((toComparewthPoly)))) {
							delete = false;
						}

					} else {
						if (!(hash[z][1].equals(t.get((int) hash[z][2])))) {
							delete = false;
						}
					}

//					if (!(hash[z][1].equals(t.get((int) hash[z][2])))) {
//						delete = false;
//					}
				}

				if (delete) {
					System.out.println("Deleted one record");
					Tuple r = tuples.remove(pointerVp.get(j).rowNumber);
					this.deleteinAllBtrees(r, pointerVp.get(j), T);

				}
				j++;

			}
			if (tuples.size() == 0) {
				T.Pages.get(T.findPageIndex(pageno)).removePage();
				T.Pages.remove(T.findPageIndex(pageno));
				T.saveTable();
			} else {
				T.Pages.get(T.findPageIndex(pageno)).saveTuples(tuples);
				T.saveTable();
			}

		}

	}

	private void updateShiftedPointersD(BTree b, Pointer newp) {
		ArrayList<Pointer> pointerV = b.getLeafPointers();
		pointerV.sort(null);

		int nOP = maxPage;
		boolean done = false;
		pointerV.add(0, newp);
		pointerV.sort(null);

		int index = pointerV.indexOf(newp) + 1;
		int pageNo = newp.pageNumber;
		while (index < pointerV.size() && done == false) {
			if (pointerV.get(index).pageNumber == pageNo) {
				pointerV.get(index).rowNumber = pointerV.get(index).rowNumber - 1;
			} else {
				done = true;
			}
			index++;
		}
	}

	private void updateShiftedPointersD(RTree b, Pointer newp) {
		ArrayList<Pointer> pointerV = b.getLeafPointers();
		pointerV.sort(null);

		int nOP = maxPage;
		boolean done = false;
		pointerV.add(0, newp);
		pointerV.sort(null);

		int index = pointerV.indexOf(newp) + 1;
		int pageNo = newp.pageNumber;
		while (index < pointerV.size() && done == false) {
			if (pointerV.get(index).pageNumber == pageNo) {
				pointerV.get(index).rowNumber = pointerV.get(index).rowNumber - 1;
			} else {
				done = true;
			}
			index++;
		}
	}

	public void deleteinAllBtrees(Tuple Tuple, Pointer p, Table t)
			throws FileNotFoundException, ClassNotFoundException, IOException, DBAppException {

		Vector<String> Index = t.bTreeIndexedOn;
		Vector<Integer> colno = t.bTreeIndexedOnCol;

		for (int i = 0; i < Index.size(); i++) {

			System.out.println("Deleted: " + Tuple.obj.get(colno.get(i)) + " in pointer: " + p);
			BTree b = LoadBtree(t.TableName, Index.get(i));
			b.deleteValue((Comparable) Tuple.obj.get(colno.get(i)), p);
			updateShiftedPointersD(b, p);
			b.saveBTree(t.TableName, Index.get(i));
		}
		Vector<String> Index2 = t.rTreeIndexedOn;
		Vector<Integer> colno2 = t.rTreeIndexedOnCol;
		for (int i = 0; i < Index2.size(); i++) {
			RTree r = loadRTree(t.TableName, Index2.get(i));
			Polygon polygon = (Polygon) Tuple.obj.get(colno2.get(i));
			Poly poly = new Poly();
			poly.p = polygon;
			r.deleteValue(poly, p);
			updateShiftedPointersD(r, p);
			r.saveRTree(t.TableName, Index2.get(i));
		}

	}

	public static ArrayList<Pointer> andingPointers(ArrayList<Pointer> t1, ArrayList<Pointer> t2) {

		int index = 0;
		ArrayList<Pointer> result = new ArrayList<Pointer>();

		for (int x = 0; x < t1.size(); x++) {

			for (int y = 0; y < t2.size(); y++) {

				if (t1.get(x).compareTo(t2.get(y)) == 0) {

					result.add(t1.get(x));
					index = index + 1;

				}

				// System.out.println("hereeeee");
			}

		}

		// Tuple[] uniqueR = new HashSet<Tuple>(Arrays.asList(result)).toArray(new
		// Tuple[0]);

		removeDuplicates(result);

		return result;

	}

	public static ArrayList<Pointer> removeDuplicates(ArrayList<Pointer> list) {

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

		list = newList;

		// return the new list
		return newList;
	}

	// MILESTONE 1 DELETE METHOD
	public void deleteFromTableO(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, IOException, ClassNotFoundException {

		Vector<String[]> metadata = getMetaData(strTableName);

		Object[][] hash = new Object[htblColNameValue.size()][3];

		if (metadata.size() == 0) {
			throw new DBAppException("error no such table");
			// return;
		}

		Enumeration<String> enumeration = htblColNameValue.keys();
		String colname = "";
		Object value;

		int j = 0;

		while (enumeration.hasMoreElements()) {
			colname = enumeration.nextElement();
			value = htblColNameValue.get(colname);

			hash[j][0] = colname;
			hash[j][1] = value;
			boolean found = false;
			for (int x = 0; x < metadata.size(); x++) {

				if (metadata.get(x)[1].equals(colname)) {
					found = true;

					String strColType = "class " + metadata.get(x)[2];
					Object strColValue = hash[j][1];

					if (strColType.equalsIgnoreCase(strColValue.getClass().toString())) {
						hash[j][2] = x;
						break;

					} else {
						throw new DBAppException("The insertion of (" + metadata.get(x)[1] + ") is not the right type");
						// return;
					}

				}
			}
			if (!found) {
				throw new DBAppException("The insertion col name does not exist");

			}
			j++;
		}

		Table T = loadTable(strTableName);

		for (int i = 0; i < T.Pages.size(); i++) {// pages
			Vector<Tuple> tuples = T.Pages.get(i).loadTuples();
			for (int k = 0; k < tuples.size(); k++) {// tuple
				Vector<Object> t = tuples.get(k).obj;

				boolean delete = true;

				for (int z = 0; z < hash.length; z++) {// hash
					if (!(hash[z][1].equals(t.get((int) hash[z][2])))) {
						delete = false;
					}
				}

				if (delete) {
					System.out.println("Deleted one record");
					tuples.remove(k);
					k--;
				}
			}

			if (tuples.size() == 0) {
				T.Pages.get(i).removePage();
				T.Pages.remove(i);
				T.saveTable();
				i = i - 1;
			} else {
				T.Pages.get(i).saveTuples(tuples);
				T.saveTable();
			}

		}

	}

	private boolean keyExists(Vector<Tuple> tuples, Tuple t) {
		for (int i = 0; i < tuples.size(); i++) {
			if (tuples.get(i).compareTo(t) == 0) {
				return true;
			}
		}
		return false;
	}

	public int checkMetaFindIndex(String strTableName, String strColName) throws IOException {

		File f = new File("data\\metadata.csv");
		BufferedReader br = new BufferedReader(new FileReader(f));
		String data;
		String all = "";
		int finalcol = -1;
		int indexedColNum = 0;
		while ((data = br.readLine()) != null) {

			String[] attributes = data.split(",");

			if (attributes[0].equals(strTableName)) {
				if (attributes[1].equalsIgnoreCase(strColName)) {
					attributes[4] = "true";
					finalcol = indexedColNum;
					data = attributes[0] + "," + attributes[1] + "," + attributes[2] + "," + attributes[3] + ","
							+ attributes[4];
				} else {
					indexedColNum++;
				}
			}
			all = all + data + "\n";
		}
		BufferedWriter writer = new BufferedWriter(new FileWriter("data\\metadata.csv"));

		writer.write(all);

		writer.close();

		return finalcol;
	}

	// New Create Btree --Can't be Created on Polygons Added
	public BTree createBTreeIndex(String strTableName, String strColName)
			throws IOException, DBAppException, ClassNotFoundException {
		int colIndex = this.checkMetaFindIndex(strTableName, strColName);
		if (colIndex == -1) {
			throw new DBAppException("This column was never found to be indexed");
		}
		Vector<String[]> metadata = getMetaData(strTableName);
		for (int m = 0; m < metadata.size(); m++) {
			if (metadata.get(m)[1].equalsIgnoreCase(strColName)) {
				String type = metadata.get(m)[2].toLowerCase();

				if (type.contains("polygon")) {

					throw new DBAppException("YOU CANNOT HAVE BTREE ON POLYGON");
				}

			}

		}

		BTree BT = new BTree();

		Table tab = this.loadTable(strTableName);
		tab.createBtreeIndex(strColName, colIndex);

		for (int i = 0; i < tab.Pages.size(); i++) {
			Vector<Tuple> t = tab.Pages.get(i).loadTuples();
			System.out.println("loading " + tab.Pages.get(i).pageName);
			for (int x = 0; x < t.size(); x++) {
				Tuple a = t.get(x);
				Pointer p = new Pointer(tab.TableName, tab.Pages.get(i).number, x);
				BT.insert((Comparable) a.obj.get(colIndex), p);
			}
		}
		BT.saveBTree(strTableName, strColName);
		return BT;

	}

	// OLD create Btree
	@SuppressWarnings({ "unchecked", "unchecked", "unchecked" })
	public BTree createBTreeIndex1(String strTableName, String strColName)
			throws IOException, DBAppException, ClassNotFoundException {
		int colIndex = this.checkMetaFindIndex(strTableName, strColName);
		if (colIndex == -1) {
			throw new DBAppException("This column was never found to be indexed");
		}

		BTree BT = new BTree();

		Table tab = this.loadTable(strTableName);
		tab.createBtreeIndex(strColName, colIndex);

		for (int i = 0; i < tab.Pages.size(); i++) {
			Vector<Tuple> t = tab.Pages.get(i).loadTuples();
			System.out.println("loading " + tab.Pages.get(i).pageName);
			for (int x = 0; x < t.size(); x++) {
				Tuple a = t.get(x);
				Pointer p = new Pointer(tab.TableName, tab.Pages.get(i).number, x);
				BT.insert((Comparable) a.obj.get(colIndex), p);
			}
		}
		BT.saveBTree(strTableName, strColName);
		return BT;

	}

//Create R tree for polygons
	public RTree createRTreeIndex(String strTableName, String strColName)
			throws IOException, DBAppException, ClassNotFoundException {
		int colIndex = this.checkMetaFindIndex(strTableName, strColName);
		if (colIndex == -1) {
			throw new DBAppException("This column was never found to be indexed");
		}
		Vector<String[]> metadata = getMetaData(strTableName);
		for (int m = 0; m < metadata.size(); m++) {
			if (metadata.get(m)[1].equalsIgnoreCase(strColName)) {
				String type = metadata.get(m)[2].toLowerCase();

				if (!(type.contains("polygon"))) {

					throw new DBAppException("YOU CANNOT HAVE rTREE ON a non-POLYGON");
				}

			}

		}

		RTree RT = new RTree();

		Table tab = this.loadTable(strTableName);

		for (int i = 0; i < tab.Pages.size(); i++) {
			Vector<Tuple> t = tab.Pages.get(i).loadTuples();
			System.out.println("loading " + tab.Pages.get(i).pageName);
			for (int x = 0; x < t.size(); x++) {
				Tuple a = t.get(x);
				Pointer p = new Pointer(tab.TableName, tab.Pages.get(i).number, x);
				Polygon p1 = (Polygon) a.obj.get(colIndex);
				Poly poly = new Poly();
				poly.p = p1;
				RT.insert(poly, p);

			}
		}
		tab.createRtreeIndex(strColName, colIndex);
		RT.saveRTree(strTableName, strColName);
		return RT;

	}

	//////////// SELECT METHOD AND HELPERSSS///////////////////////

	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws Exception {

		if (arrSQLTerms.length != strarrOperators.length + 1) {

			throw new DBAppException(
					"Lengths of arrSQLTerms and strarrOperators are not compitable, length of arrSQLTerms must be equal length of strarrOperators +1 ");
		}

		String TableName;
		String ColName;
		Object value;
		String operator;

		boolean clustering = false;
		boolean indexed = false;
		boolean found = false;

		// Declaring 2-D array with arrSQLTerms rows
		// Tuple arr[][] = new Tuple[arrSQLTerms.length][];
		Vector<Tuple[]> totaltuples = new Vector();
		// ArrayList<Tuple []> totaltuples = new ArrayList<Tuple[]>();

		for (int i = 0; i < arrSQLTerms.length; i++) {

			TableName = arrSQLTerms[i]._strTableName;
			ColName = arrSQLTerms[i]._strColumnName;
			operator = arrSQLTerms[i]._strOperator;
			value = arrSQLTerms[i]._objValue;
			Vector<String[]> metadata = getMetaData(TableName);

			found = false;
			indexed = false;
			clustering = false;

			File tmpDir = new File("data\\" + TableName + ".class");
			boolean exists = tmpDir.exists();

			if (!exists) {

				throw new DBAppException("Table Name is incorrect");
			}

			ObjectInputStream input = new ObjectInputStream(new FileInputStream("data//" + TableName + ".class")); // get
			// the
			// table's
			// file
			Table T = (Table) input.readObject(); // deserialize the table's object
			input.close();

			// int cc = -1;
			String currentcolname = "";
			String currentvalue = "";
			int colpos = 0;

			for (int m = 0; m < metadata.size(); m++) {

				if (metadata.get(m)[0].equalsIgnoreCase(TableName) && metadata.get(m)[1].equalsIgnoreCase(ColName)) {

					found = true;
					colpos = m;

					String coltype = metadata.get(m)[2].toLowerCase();
					// try {

					// check if it's indexed

					if (metadata.get(m)[4].equalsIgnoreCase("true")) {

						indexed = true;

					}

					// check if it's clustering key

					if (metadata.get(m)[3].equalsIgnoreCase("true")) {

						clustering = true;

					}

					if (indexed == false) {
						// not indexed

						if (clustering == false) {
							// not clustering key and not indexed
							// linear search

							Tuple[] resultingtuples = linearsearch(T, ColName, coltype, value, operator, colpos);

							totaltuples.add(resultingtuples);

						}

						else {
							// clustering key and not indexed
							// binary search

							Vector<Tuple> tuplesperterm = new Vector();

							for (int j = 0; j < T.Pages.size(); j++) {

								// Vector<Tuple> tuplesperpage = new Vector();
								ObjectInputStream inputpage = new ObjectInputStream(
										new FileInputStream("data\\" + T.TableName + T.Pages.get(j).number + ".class")); // get
								// the
								// table's
								// file
								Vector<Tuple> tuples = (Vector<Tuple>) inputpage.readObject(); // deserialize
																								// the
																								// table's
																								// object
								inputpage.close();
								// System.out.println(tuples.get(0).obj.get(1));

								// tuplesperpage = binarySearchSelect(tuples, 0, tuples.size(), value, colpos,
								// operator);

								tuplesperterm
										.addAll(binarySearchSelect(tuples, 0, tuples.size(), value, colpos, operator));

							}

							Tuple[] resultingtuples = new Tuple[tuplesperterm.size()];

							for (int z = 0; z < tuplesperterm.size(); z++) {

								resultingtuples[z] = tuplesperterm.get(z);
							}

							totaltuples.add(resultingtuples);

						}

					}

					else {

						Vector<overFlowPointers> pointers = new Vector();

						// indexed
						if (coltype.contains("polygon")) {

							System.out.println("af" + coltype);

							// RTree

							RTree b = loadRTree(TableName, ColName);
							Poly poly = new Poly();
							try {
								Polygon d = (Polygon) value;
								poly.p = d;

							} catch (Exception e) {
								// TODO: handle exception
								throw new DBAppException("Input Type is not right");
							}

							pointers = b.searchselect(poly, operator);

//
						}

						else {
							// B+Tree

							Object value1;

							BTree b = LoadBtree(TableName, ColName);
							pointers = typecasting(value, coltype, operator, b);

							// System.out.println("POINTER: " + pointers);

						}

						Vector<Tuple> tuplesperterm = new Vector();

						for (int big = 0; big < pointers.size(); big++) {

							for (int small = 0; small < pointers.get(big).values.size(); small++) {

								int pageNum = ((Pointer) pointers.get(big).values.get(small)).pageNumber;
								int rowNum = ((Pointer) pointers.get(big).values.get(small)).rowNumber;
								ObjectInputStream inputpage12 = new ObjectInputStream(
										new FileInputStream("data\\" + T.TableName + pageNum + ".class"));

								Vector<Tuple> inpage = (Vector<Tuple>) inputpage12.readObject(); // deserialize

								inputpage12.close();

								tuplesperterm.add(inpage.get(rowNum));

							}
						}

						if (coltype.contains("polygon") && indexed == true) {

							Poly poly = new Poly();
							Polygon d = (Polygon) value;
							poly.p = d;

							tuplesperterm = poly.compareToPOLY(tuplesperterm, operator, colpos, poly);

						}

						Tuple[] resultingtuples = new Tuple[tuplesperterm.size()];

						for (int z = 0; z < tuplesperterm.size(); z++) {

							resultingtuples[z] = tuplesperterm.get(z);
						}

						totaltuples.add(resultingtuples);
						// System.out.println(totaltuples.size());

					}

				}

			}

			if (found == false) {
				System.out.println("Table Name or ColName Incorrect");

			}
		}

		// perform OPERATORSS

		for (int y = 0; y < strarrOperators.length; y++) {

//			 for (int r = 0; r < (totaltuples.size())-1 ; r++) {
//
//			for (int gg = 0; gg < totaltuples.get(r).length; gg++) {
//
//				System.out.println("At round= " + r + " At index=" + gg + totaltuples.get(r)[gg]);
//			}
//			 }

			if (strarrOperators[y].equalsIgnoreCase("and")) {

				// System.out.println("rghdrh" + totaltuples.get(y));

				Vector<Tuple> tempvector = anding(totaltuples.get(y), totaltuples.get(y + 1));

				// System.out.println("VECTORR" + tempvector.toString());

				Tuple[] temparray = new Tuple[tempvector.size()];

				for (int q = 0; q < temparray.length; q++) {

					temparray[q] = tempvector.get(q);

				}

				// System.out.println("VECORR2" + totaltuples.get(y + 1).length);

				totaltuples.set(y + 1, temparray);

				// System.out.println("VECORR2" + totaltuples.get(y + 1).length);

			}

			else if (strarrOperators[y].equalsIgnoreCase("or")) {

				Vector<Tuple> tempvector = oring(totaltuples.get(y), totaltuples.get(y + 1));

				Tuple[] temparray = new Tuple[tempvector.size()];

				for (int q = 0; q < temparray.length; q++) {

					temparray[q] = tempvector.get(q);
				}

				totaltuples.set(y + 1, temparray);

			}

			else if (strarrOperators[y].equalsIgnoreCase("xor")) {

				Vector<Tuple> tempvector = xoring(totaltuples.get(y), totaltuples.get(y + 1));

				Tuple[] temparray = new Tuple[tempvector.size()];

				for (int q = 0; q < temparray.length; q++) {

					temparray[q] = tempvector.get(q);
				}

				totaltuples.set(y + 1, temparray);

			}

		}

		Tuple[] finalRESULT = new Tuple[totaltuples.get(totaltuples.size() - 1).length];
		finalRESULT = totaltuples.get(totaltuples.size() - 1);

		System.out.println("FINALLL size " + finalRESULT.length);

		// Getting Iterator

		Iterator<Tuple> iterator = Arrays.stream(finalRESULT).iterator();

		// Iterator Tuple[] TuplesIterator = finalRESULT.iterator();

		// just for testing
//		for (int i = 0; i < totaltuples.size(); i++) {
//
//			System.out.println("ROW" + i + " ");
//			for (int j = 0; j < totaltuples.get(i).length; j++) {
//				System.out.println("LOOOOOOKKKKKK" + i + (totaltuples.get(i))[j]);
//			}
//
//		}
		//////////////////////// WOHOOO///////////////////////

		return iterator;
	}

	public int gettingpos(Table T, String ColName) {

		int colpos = 0;

// keytype

		return colpos;

	}

	public Tuple[] linearsearch(Table T, String ColName, String coltype, Object value, String operator, int colpos)
			throws Exception, IOException {

		Vector<Tuple> resultV = new Vector();
		// Tuple [] resultA;
		// resultA[0]=
		// int j = 0;
		String currentcolname = "";
		String currentvalue = "";

		// Type casting the value to its type to be able to compare it

		try {

			if (coltype.contains("integer")) {

				value = (Integer) value;

			} else if (coltype.contains("double")) {

				value = (Double) value;

			} else if (coltype.contains("string")) {

				value = (String) value;

			} else if (coltype.contains("boolean")) {

				value = (boolean) value;

			} else if (coltype.contains("polygon")) {

				value = (Polygon) value;

			} else if (coltype.contains("date")) {

				value = (Date) value;
			}
		} catch (Exception e) {
			// TODO: handle exception
			throw new DBAppException("Input Type is not right");
		}
		for (int i = 0; i < T.Pages.size(); i++) {

			ObjectInputStream inputpage = new ObjectInputStream(
					new FileInputStream("data\\" + T.TableName + T.Pages.get(i).number + ".class")); // get
			// the
			// table's
			// file
			Vector<Tuple> tuples = (Vector<Tuple>) inputpage.readObject(); // deserialize
																			// the
																			// table's
																			// object
			inputpage.close();

			for (int y = 0; y < tuples.size(); y++) {

				// if find the tuple i want

				if (operator.equals("=")) {

					if ((tuples.get(y)).compareToSelect(value, colpos, operator) == 0) {

						resultV.add(tuples.get(y));
						// System.out.println("yesss");
					}

				} else if (operator.equals("<")) {

					if ((tuples.get(y)).compareToSelect(value, colpos, operator) < 0) {

						resultV.add(tuples.get(y));
					}

				} else if (operator.equals(">")) {

					if ((tuples.get(y)).compareToSelect(value, colpos, operator) > 0) {

						resultV.add(tuples.get(y));
					}

				} else if (operator.equals("!=")) {

					if ((tuples.get(y)).compareToSelect(value, colpos, operator) != 0) {

						resultV.add(tuples.get(y));
					}

				} else if (operator.equals("<=")) {

					if ((tuples.get(y)).compareToSelect(value, colpos, operator) == 0
							|| (tuples.get(y)).compareToSelect(value, colpos, operator) < 0) {

						resultV.add(tuples.get(y));
					}

				} else if (operator.equals(">=")) {

					if ((tuples.get(y)).compareToSelect(value, colpos, operator) == 0
							|| (tuples.get(y)).compareToSelect(value, colpos, operator) > 0) {

						resultV.add(tuples.get(y));
					}

				}

				// result.add(tuples.get(0));

			}

		}

		Tuple[] resultA = new Tuple[resultV.size()];

		for (int x = 0; x < resultV.size(); x++) {
			resultA[x] = resultV.get(x);
		}

		return resultA;
	}

	public Vector<Tuple> binarySearchSelect(Vector<Tuple> tuples, int l, int r, Object value, int colpos,
			String operator) throws DBAppException {
		Vector<Tuple> result = new Vector();

		if (r >= l) {

			int mid = l + (r - l) / 2;

			// System.out.println(tuples.get(mid).obj);
			// If the element is present at the
			// middle itself

			if (operator.equals("=")) {

				if (tuples.get(mid).compareToSelect(value, colpos, operator) == 0) {

					// System.out.println("equal");

					result.add(tuples.get(mid));

					if (mid - 1 > -1) {

						result.addAll(binarySearchSelect(tuples, l, mid - 1, value, colpos, operator));

					}

					if (mid + 1 < tuples.size()) {

						result.addAll(binarySearchSelect(tuples, mid + 1, r, value, colpos, operator));
					}

				}

				else if (tuples.get(mid).compareToSelect(value, colpos, operator) > 0 && mid - 1 > -1) {

					result.addAll(binarySearchSelect(tuples, l, mid - 1, value, colpos, operator));

				}

				else if (tuples.get(mid).compareToSelect(value, colpos, operator) < 0 && mid + 1 < tuples.size()) {

					// System.out.println("smaller");
					result.addAll(binarySearchSelect(tuples, mid + 1, r, value, colpos, operator));
				}
			}

			else if (operator.equals("<")) {

				if (tuples.get(mid).compareToSelect(value, colpos, operator) == 0 && mid - 1 > -1) {

					result.addAll(binarySearchSelect(tuples, l, mid - 1, value, colpos, operator));

				}

				else if (tuples.get(mid).compareToSelect(value, colpos, operator) > 0 && mid - 1 > -1) {

					result.addAll(binarySearchSelect(tuples, l, mid - 1, value, colpos, operator));
				}

				else if (tuples.get(mid).compareToSelect(value, colpos, operator) < 0) {

					result.add(tuples.get(mid));

					if (mid - 1 > -1) {

						result.addAll(binarySearchSelect(tuples, l, mid - 1, value, colpos, operator));

					}

					if (mid + 1 < tuples.size()) {

						result.addAll(binarySearchSelect(tuples, mid + 1, r, value, colpos, operator));
					}

				}

			}

			else if (operator.equals(">")) {

				if (tuples.get(mid).compareToSelect(value, colpos, operator) == 0 && mid + 1 < tuples.size()) {

					result.addAll(binarySearchSelect(tuples, mid + 1, r, value, colpos, operator));

				}

				else if (tuples.get(mid).compareToSelect(value, colpos, operator) > 0) {

					result.add(tuples.get(mid));

					if (mid - 1 > -1) {

						result.addAll(binarySearchSelect(tuples, l, mid - 1, value, colpos, operator));

					}

					if (mid + 1 < tuples.size()) {

						result.addAll(binarySearchSelect(tuples, mid + 1, r, value, colpos, operator));
					}
				}

				else if (tuples.get(mid).compareToSelect(value, colpos, operator) < 0 && mid + 1 < tuples.size()) {

					result.addAll(binarySearchSelect(tuples, mid + 1, r, value, colpos, operator));

				}

			}

			else if (operator.equals("<=")) {

				if (tuples.get(mid).compareToSelect(value, colpos, operator) == 0) {

					result.add(tuples.get(mid));

					if (mid - 1 > -1) {

						result.addAll(binarySearchSelect(tuples, l, mid - 1, value, colpos, operator));
					}

					if (mid + 1 < tuples.size()) {

						result.addAll(binarySearchSelect(tuples, mid + 1, r, value, colpos, operator));
					}

				}

				else if (tuples.get(mid).compareToSelect(value, colpos, operator) > 0 && mid - 1 > -1) {

					result.addAll(binarySearchSelect(tuples, l, mid - 1, value, colpos, operator));
				}

				else if (tuples.get(mid).compareToSelect(value, colpos, operator) < 0) {

					result.add(tuples.get(mid));

					if (mid - 1 > -1) {

						result.addAll(binarySearchSelect(tuples, l, mid - 1, value, colpos, operator));
					}

					if (mid + 1 < tuples.size()) {

						result.addAll(binarySearchSelect(tuples, mid + 1, r, value, colpos, operator));

					}

				}
			}

			else if (operator.equals(">=")) {

				if (tuples.get(mid).compareToSelect(value, colpos, operator) == 0) {

					result.add(tuples.get(mid));

					if (mid - 1 > -1) {

						result.addAll(binarySearchSelect(tuples, l, mid - 1, value, colpos, operator));

					}

					if (mid + 1 < tuples.size()) {

						result.addAll(binarySearchSelect(tuples, mid + 1, r, value, colpos, operator));

					}

				}

				else if (tuples.get(mid).compareToSelect(value, colpos, operator) > 0) {

					result.add(tuples.get(mid));

					if (mid - 1 > -1) {

						result.addAll(binarySearchSelect(tuples, l, mid - 1, value, colpos, operator));

					}

					if (mid + 1 < tuples.size()) {
						result.addAll(binarySearchSelect(tuples, mid + 1, r, value, colpos, operator));

					}
				}

				else if (tuples.get(mid).compareToSelect(value, colpos, operator) < 0 && mid + 1 < tuples.size()) {

					result.addAll(binarySearchSelect(tuples, mid + 1, r, value, colpos, operator));

				}

			}

			else if (operator.equals("!=")) {

				// System.out.println("hfjm" + tuples.get(mid));

				if (tuples.get(mid).compareToSelect(value, colpos, operator) != 0) {

					result.add(tuples.get(mid));

					if (mid - 1 > -1) {

						result.addAll(binarySearchSelect(tuples, l, mid - 1, value, colpos, operator));
					}

					if (mid + 1 < tuples.size()) {
						result.addAll(binarySearchSelect(tuples, mid + 1, r, value, colpos, operator));

					}

				}

				else {

					if (mid - 1 > -1) {

						result.addAll(binarySearchSelect(tuples, l, mid - 1, value, colpos, operator));
					}

					if (mid + 1 < tuples.size()) {
						result.addAll(binarySearchSelect(tuples, mid + 1, r, value, colpos, operator));

					}

				}

			} else {
				System.out.println("WRONG OPERATOR, please use '<=' instead of '=<' ");
			}

		}

		// We reach here when element is not present
		// in array
		return result;
	}

	public void removeDuplicates(Vector<Tuple> v) {

		for (int i = 0; i < v.size(); i++) {
			for (int j = 0; j < v.size(); j++) {
				if (i != j) {

					if (v.get(i).leehhh(v.get(j))) {

						v.removeElementAt(j);
					}
				}
			}
		}
	}

	public Vector<overFlowPointers> typecasting(Object value, String coltype, String operator, BTree b) throws DBAppException {

		Vector<overFlowPointers> result = new Vector();

		try {

			if (coltype.contains("integer")) {

				int value0 = (Integer) value;
				result.addAll(b.searchselect(value0, operator));

			} else if (coltype.contains("double")) {

				Double value0 = (Double) value;
				result.addAll(b.searchselect(value0, operator));

			} else if (coltype.contains("string")) {

				String value0 = (String) value;
				result.addAll(b.searchselect(value0, operator));

				// b.printTree();
				// System.out.println("INSHALLAH" + b.searchselect(value0, operator));

			} else if (coltype.contains("boolean")) {

				Boolean value0 = (boolean) value;
				result.addAll(b.searchselect(value0, operator));

			}

			// not suree
			else if (coltype.contains("date")) {

				String key = (String) value;
				String[] s = key.split("-");
				Date d = new Date(Integer.parseInt(s[0]), Integer.parseInt(s[1]), Integer.parseInt(s[2]));

				result.addAll(b.searchselect(d, operator));

			}

		}

		catch (Exception e) {
			// TODO: handle exception
			throw new DBAppException("Input Type is not right");
		}

		return result;

	}

	public Vector<Tuple> xoring(Tuple[] t1, Tuple[] t2) {

		int index = 0;
		Vector<Tuple> result = new Vector();
		boolean found = false;

		for (int x = 0; x < t1.length; x++) {

			found = false;

			for (int y = 0; y < t2.length; y++) {

				if (t1[x].leehhh(t2[y])) {

					found = true;
					break;

				}

			}

			if (found == false) {

				result.add(t1[x]);
				index = index + 1;

			}
		}

		for (int x = 0; x < t2.length; x++) {

			found = false;

			for (int y = 0; y < t1.length; y++) {

				if (t2[x].leehhh(t1[y])) {

					found = true;

				}

			}

			if (found == false) {
				result.add(t2[x]);
				index = index + 1;

			}
		}

		// Tuple[] uniqueR = new HashSet<Tuple>(Arrays.asList(result)).toArray(new
		// Tuple[0]);

		removeDuplicates(result);

		return result;

	}

	public Vector<Tuple> anding(Tuple[] t1, Tuple[] t2) {

		int index = 0;
		Vector<Tuple> result = new Vector();

		for (int x = 0; x < t1.length; x++) {

			for (int y = 0; y < t2.length; y++) {

				if (t1[x].leehhh(t2[y])) {

					result.add(t1[x]);
					index = index + 1;

				}

				// System.out.println("hereeeee");
			}

		}

		// Tuple[] uniqueR = new HashSet<Tuple>(Arrays.asList(result)).toArray(new
		// Tuple[0]);

		removeDuplicates(result);

		return result;

	}

	public Vector<Tuple> oring(Tuple[] t1, Tuple[] t2) {

		int index = 0;
		Vector<Tuple> result = new Vector();
		// Tuple[] result = new Tuple[t1.size() + t2.size()];

		for (int x = 0; x < t1.length; x++) {

			result.add(t1[x]);
			index = index + 1;

		}

		for (int y = 0; y < t2.length; y++) {

			result.add(t2[y]);
			index = index + 1;
			// t1.addAll(t2);

		}

		// Tuple[] uniqueR = new HashSet<Tuple>(Arrays.asList(result)).toArray(new
		// Tuple[0]);

		removeDuplicates(result);

		return result;
	}

	//////////////////////////////////////////////////////////////////

	// Helper method for update

	///////////////////////////////////////////////////////////////

	

//////////////////////////////////////////////////////////////////////////////////////////////////////////
	public static Polygon makePolygon(String s) throws DBAppException {
		try {
			int size = 0;
			for (int i = 0; i < s.length(); i++) {
				if (s.charAt(i) == ')') {
					size++;

				}
			}
			int c = 0;
			int x[] = new int[size];
			;
			int y[] = new int[size];
			;
			for (int i = 0; i < s.length(); i++) {

				if (s.charAt(i) == '(') {
					i++;
					// System.out.println("the length is "+x.length);
					String rakm = "";
					while (s.charAt(i) != ',') {
						rakm = rakm + s.charAt(i);
						i++;

					}
					x[c] = Integer.parseInt(rakm);
					c++;

				}
			}
			// System.out.println("ANA HENA"+Arrays.toString(x));
			c = 0;
			for (int i = 0; i < s.length(); i++) {
				// System.out.println("the i"+i);
				if (s.charAt(i) == ')') {
					String rakm = "";

					int j = i - 1;
					while (s.charAt(j) != ',') {
						rakm = s.charAt(j) + rakm;
						j--;
					}
					y[c] = Integer.parseInt(rakm);
					c++;
				}

			}

			// System.out.println("x's"+Arrays.toString(x)+" y's"+Arrays.toString(y));
			return new Polygon(x, y, x.length);
		} catch (Exception e) {
			// TODO: handle exception
			throw new DBAppException("Input is not right");
		}

	}

///////////////////////////////////////////////////////////////////////////////////////////////////////////
	// Old Update Table
	public void updateTable1(String strTableName, String strKey, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, FileNotFoundException, IOException, Exception {

		Enumeration<String> enumeration = htblColNameValue.keys();
		String colname = "";
		Object value;
		int j = 0;
		Object[][] hash = new Object[htblColNameValue.size()][2];
		String keytype = "";
		Boolean replaced = false;

		Vector<String[]> metadata = getMetaData(strTableName);

		String keycol = getKey(metadata);
		ObjectInputStream input = new ObjectInputStream(new FileInputStream("data//" + strTableName + ".class")); // get
																													// the
																													// table's
																													// file
		Table T = (Table) input.readObject(); // deserialize the table's object
		input.close();

		// keytype
		boolean isPolygon = false;
		keytype = metadata.get(0)[2].toLowerCase();
		if (keytype.contains("polygon")) {
			isPolygon = true;
		}

		while (enumeration.hasMoreElements()) {
			colname = enumeration.nextElement();
			value = htblColNameValue.get(colname);

			hash[j][0] = colname;
			hash[j][1] = value;

			j++;
		}
		boolean done = false;
		for (int i = 1; i <= T.Pages.size() && done == false; i++) {
			Vector<Integer> allpos = new Vector<Integer>();
			ObjectInputStream inputpage = new ObjectInputStream(
					new FileInputStream("data\\" + T.TableName + i + ".class")); // get
																					// the
																					// table's
																					// file
			Vector<Tuple> tuples = (Vector<Tuple>) inputpage.readObject(); // deserialize
																			// the
																			// table's
																			// object
			inputpage.close();

			String dateString = "";
			int pos = binarySearch(tuples, 0, tuples.size() - 1, strKey, keytype);
			allpos.add(pos);
			if (pos != -1) {
				// String dateClustringkey = strKey;
				// handling duplicates
				// going forward starting from the pos where i found my clustering key using
				// binary Search until I hit a different key
				// when I find one different key this means that there are no more duplicates,
				// thus I am "done" searching through all pages
				for (int forw = pos + 1; forw < tuples.size(); forw++) {
					String c = "";
					if (isPolygon) {
						int x[] = ((Polygon) ((tuples.get(forw)).obj.get(0))).xpoints;
						int arr[] = ((Polygon) ((tuples.get(forw)).obj.get(0))).ypoints;
						c = makeIntoString(x, arr);
					} else if (keytype.contains("date")) {
						String strClustringkey = strKey;

						String[] s = strClustringkey.split("-");
						// handling the toString of dates as the input clustering key is in a specific
						// format
						Date d = new Date(Integer.parseInt(s[0]), Integer.parseInt(s[1]), Integer.parseInt(s[2]));
						Date d2 = (Date) ((tuples.get(forw)).obj.get(0));
						c = d2.toString();
						dateString = d.toString();

					}

					else {
						c = ((tuples.get(forw)).obj.get(0)).toString();
						System.out.println("The c is:" + c + " Thestrkey is:" + strKey);
					}
					if (c.equalsIgnoreCase(strKey) || c.equals(dateString)) {
						allpos.add(forw);

					} else {
						done = true;
						break;
					}

				}
				for (int back = pos - 1; back >= 0; back--) {
					// handling duplicates
					// going back starting from the pos where i found my clustering key using binary
					// Search until I hit a different key
					String c = "";
					if (isPolygon) {
						int x[] = ((Polygon) ((tuples.get(back)).obj.get(0))).xpoints;
						int arr[] = ((Polygon) ((tuples.get(back)).obj.get(0))).ypoints;
						c = makeIntoString(x, arr);
					} else if (keytype.contains("date")) {
						String strClustringkey = strKey;

						String[] s = strClustringkey.split("-");

						Date d = new Date(Integer.parseInt(s[0]), Integer.parseInt(s[1]), Integer.parseInt(s[2]));
						Date d2 = (Date) ((tuples.get(back)).obj.get(0));
						c = d2.toString();
						dateString = d.toString();

					}

					else {
						c = ((tuples.get(back)).obj.get(0)).toString();
					}
					if (c.equalsIgnoreCase(strKey) || c.equals(dateString)) {
						allpos.add(back);

					} else {

						break;
					}

				}
				replaced = true;

				/////
				for (int go = 0; go < allpos.size(); go++) {
					pos = allpos.elementAt(go);
					tuples.get(pos).obj.set(tuples.get(pos).obj.size() - 1,
							new SimpleDateFormat("yyyy-MM-dd:HH-mm-ss").format(new Date()));

					for (int k = 0; k < hash.length; k++) {

						if (keycol.equalsIgnoreCase((String) hash[k][0])) {

							throw new DBAppException("please remove the clustering key from cols to be updated.");

						}
						String search = (String) hash[k][0];
						for (int m = 0; m < metadata.size(); m++) {
							if (metadata.get(m)[1].equalsIgnoreCase(search)) {
								String type = metadata.get(m)[2].toLowerCase();
								try {
									if (type.contains("integer")) {
										tuples.get(pos).obj.set(m, (Integer) hash[k][1]);
									} else if (type.contains("double")) {
										tuples.get(pos).obj.set(m, (Double) hash[k][1]);

									} else if (type.contains("string")) {
										tuples.get(pos).obj.set(m, (String) hash[k][1]);

									} else if (type.contains("boolean")) {
										tuples.get(pos).obj.set(m, (Boolean) hash[k][1]);

									} else if (type.contains("polygon")) {
										tuples.get(pos).obj.set(m, (Polygon) hash[k][1]);

									} else if (type.contains("date")) {
										tuples.get(pos).obj.set(m, (Date) hash[k][1]);

									}
								} catch (ClassCastException e) {
									System.out.println("You entered a wrong type");
								}
							}

						}
					}

					ObjectOutputStream output = new ObjectOutputStream(
							new FileOutputStream("data\\" + T.TableName + i + ".class"));
					output.writeObject(tuples);// rewrites the pages back
					output.flush();
					output.close();

					// break;
				}
			}

		}

		if (replaced == false) {
			throw new DBAppException("This key was never found");
		}

	}

	// NEW Sandy "This Method Prints Out The Page No bas lazem ta5od the first Key
	// "awel item f awel el Btree"
	public void GetMeallRecordsPageCheck(String tableName, String Index, Comparable key)
			throws ClassNotFoundException, IOException {
		BTree b = LoadBtree(tableName, Index);
		BTreeLeafNode n = b.findLeafNodeShouldContainKey(key);

		while (n != null) {
			Object[] keys = n.keys;

			for (int i = 0; i < keys.length; i++) {
				if (keys[i] != null) {
					overFlowPointers p = b.search((Comparable) keys[i]);
					System.out.print("This Value " + keys[i] + " Should be in " + "\n");

					for (int j = 0; j < p.values.size(); j++) {
						System.out.print("Page:" + ((Pointer) p.values.get(j)).pageNumber + " R"
								+ ((Pointer) p.values.get(j)).rowNumber + "\n");
					}

				}
			}

			n = (BTreeLeafNode) n.rightSibling;
		}
	}

	public static int calArea(int[] x, int[] y) {

		Polygon p1 = new Polygon(x, y, x.length);

		Dimension dim = p1.getBounds().getSize();
		int Areap1 = dim.width * dim.height;
		return Areap1;
		// System.out.println(Areap1);
	}

	public void GetMeallR(String tableName, String Index, Poly key)
			throws ClassNotFoundException, IOException, DBAppException {
		RTree b = loadRTree(tableName, Index);
		BTreeLeafNode n = b.findLeafNodeShouldContainKey(key);

		while (n != null) {
			Object[] keys = n.keys;

			for (int i = 0; i < keys.length; i++) {
				if (keys[i] != null) {
					overFlowPointers p = b.search((Poly) keys[i]);
					int area = calArea(((Poly) keys[i]).p.xpoints, ((Poly) keys[i]).p.ypoints);
					String s = makeIntoString(((Poly) keys[i]).p.xpoints, ((Poly) keys[i]).p.ypoints);
					System.out.print("This Value of area " + area + " points " + s + " Should be in " + "\n");

					for (int j = 0; j < p.values.size(); j++) {
						System.out.print("Page:" + ((Pointer) p.values.get(j)).pageNumber + " R"
								+ ((Pointer) p.values.get(j)).rowNumber + "\n");
					}

				}
			}

			n = (BTreeLeafNode) n.rightSibling;
		}
	}

///////////////////////////////Helper Method for Update///////////////////////////////////////////////////////////////////////
	public void updateIndices(Object[][] hash, Table T, Pointer p, Tuple oldTuple)
			throws ClassNotFoundException, IOException, DBAppException {
			try {
				for (int i = 0; i < hash.length; i++) {
					
					for (int j = 0; j < T.bTreeIndexedOn.size(); j++) {
						
						if (T.bTreeIndexedOn.elementAt(j).equalsIgnoreCase((String) hash[i][0])) {
							String colName = (String) hash[i][0];
							BTree b = LoadBtree(T.TableName, colName);
							System.out.println("this should work: " + oldTuple.obj.elementAt(T.bTreeIndexedOnCol.elementAt(j)));
							System.out.println(p);
							System.out.println("UPDATING INDEX");
							b.deleteValue((Comparable) (oldTuple.obj.elementAt(T.bTreeIndexedOnCol.elementAt(j))), p);
							b.insert((Comparable) hash[i][1], p);

							b.saveBTree(T.TableName, colName);

						}
					}

				}

				for (int i = 0; i < hash.length; i++) {
					for (int j = 0; j < T.rTreeIndexedOn.size(); j++) {

						if (T.rTreeIndexedOn.elementAt(j).equalsIgnoreCase((String) hash[i][0])) {
							String colName = (String) hash[i][0];
							RTree b = loadRTree(T.TableName, colName);
							System.out.println(
									"this should work for rtree: " + oldTuple.obj.elementAt(T.rTreeIndexedOnCol.elementAt(j)));
							System.out.println(p);

							Polygon p1 = (Polygon) (oldTuple.obj.elementAt(T.rTreeIndexedOnCol.elementAt(j)));
							Poly poly = new Poly();
							poly.p = p1;
							System.out.println("UPDATING RTREE INDEX");
							b.deleteValue(poly, p);

							Polygon p2 = (Polygon) hash[i][1];
							Poly poly2 = new Poly();
							poly2.p = p2;
							b.insert(poly2, p);

							b.saveRTree(T.TableName, colName);

						}
					}

				}
				
			} catch (ClassCastException e) {
				// TODO: handle exception
				throw new DBAppException("Incorrect type");
			}



	}
	
	///////////////////////////////////////////////////////////////////////////////////////////////////////////
	public void updateTable(String strTableName, String strKey, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, FileNotFoundException, IOException, Exception {
		Enumeration<String> enumeration;
		try {
			enumeration = htblColNameValue.keys();
		} catch (NullPointerException e) {
			throw new DBAppException("INPUT IS INCORRECT");
		}
		String colname = "";
		Object value;
		int j = 0;
		Object[][] hash = new Object[htblColNameValue.size()][2];
		String keytype = "";
		Boolean replaced = false;

		Vector<String[]> metadata = getMetaData(strTableName);

		String keycol = getKey(metadata);
		ObjectInputStream input;
		Table T;
		try {
			input = new ObjectInputStream(new FileInputStream("data//" + strTableName + ".class")); // get
// the
// table's
// file
			T = (Table) input.readObject(); // deserialize the table's object
			input.close();
		} catch (FileNotFoundException e) {
			throw new DBAppException("TABLE NOT FOUND");
		}
// keytype
		boolean isPolygon = false;
		keytype = metadata.get(0)[2].toLowerCase();
		if (keytype.contains("polygon")) {
			isPolygon = true;
		}

		while (enumeration.hasMoreElements()) {
			colname = enumeration.nextElement();
			value = htblColNameValue.get(colname);

			hash[j][0] = colname;
			hash[j][1] = value;

			j++;
		}
//if there is an index on clustering key use b+ tree in search!
		boolean indexOnClstr = false;
		for (int q = 0; q < T.bTreeIndexedOn.size(); q++) {
			if (keycol.equalsIgnoreCase(T.bTreeIndexedOn.get(q))) {
				indexOnClstr = true;
			}
		}

		for (int q = 0; q < T.rTreeIndexedOn.size(); q++) {
			if (keycol.equalsIgnoreCase(T.rTreeIndexedOn.get(q))) {
				indexOnClstr = true;
			}
		}

////
////d
////
//return 
		if (indexOnClstr) {
			System.out.println("I AM HERE");
			overFlowPointers pointers;
			if (isPolygon) {
				overFlowPointers pointers1;
				RTree b = loadRTree(strTableName, keycol);
				Poly poly = new Poly();
				Polygon d = makePolygon(strKey);
				poly.p = d;
				pointers1 = b.search(poly);
				pointers = new overFlowPointers();
				try {
					if (pointers1.values.isEmpty()) {

					}
				} catch (NullPointerException e) {
					// TODO: handle exception
					throw new DBAppException("key not found");
				}
				Vector<Integer> toremove = new Vector<Integer>();
				int size = pointers1.values.size();
				for (int last = 0; last < size; last++) {
					int lpNum = ((Pointer) pointers1.values.get(last)).pageNumber; // tableName+pageNum
					int lrowNum = ((Pointer) pointers1.values.get(last)).rowNumber;

					ObjectInputStream inputpage123 = new ObjectInputStream(
							new FileInputStream("data\\" + T.TableName + lpNum + ".class")); // get
					// the
					// table's
					// file
					Vector<Tuple> tuples = (Vector<Tuple>) inputpage123.readObject(); // deserialize
					// the
					// table's
					// object
					inputpage123.close();

					Polygon here = (Polygon) tuples.get(lrowNum).obj.elementAt(0);
					System.out.println("HEREE :" + makeIntoString(here.xpoints, here.ypoints) + "  " + strKey);
					// Arrays.equals(here.xpoints, d.xpoints)==false && Arrays.equals(here.ypoints,
					// d.ypoints)==false
					if ((makeIntoString(here.xpoints, here.ypoints).equals(strKey))) {
						// pointers.values.remove(last);
						// toremove.add(last);
						// System.out.println("after removee "+pointers+last);
						pointers.values.add(pointers1.values.get(last));
					}

				}
			} else {
				BTree b = LoadBtree(strTableName, keycol);
				pointers = searchBtreeHelper(strKey, keytype, b);
				try {
					if (pointers.values.isEmpty()) {

					}
				} catch (NullPointerException e) {
					// TODO: handle exception
					throw new DBAppException("key not found");
				}
			}

			for (int p = 0; p < pointers.values.size(); p++) {
				int pNum = ((Pointer) pointers.values.get(p)).pageNumber; // tableName+pageNum
				int rowNum = ((Pointer) pointers.values.get(p)).rowNumber;
				ObjectInputStream inputpage12 = new ObjectInputStream(
						new FileInputStream("data\\" + T.TableName + pNum + ".class")); // get
// the
// table's
// file
				Vector<Tuple> tuples = (Vector<Tuple>) inputpage12.readObject(); // deserialize
// the
// table's
// object
				inputpage12.close();

///////////////////////
/////
				for (int hadto = 0; hadto < hash.length; hadto++) {
					if (keycol.equalsIgnoreCase((String) hash[hadto][0])) {

						throw new DBAppException("please remove the clustering key from cols to be updated.");

					}
				}
				replaced = true;

				tuples.get(rowNum).obj.set(tuples.get(rowNum).obj.size() - 1,
						new SimpleDateFormat("yyyy-MM-dd:HH-mm-ss").format(new Date()));
				System.out.println(((Pointer) pointers.values.get(p)) + "    " + tuples.get(rowNum)
						+ "this is what went thro updateimdice witl clstr index");
				updateIndices(hash, T, ((Pointer) pointers.values.get(p)), tuples.get(rowNum));

				for (int k = 0; k < hash.length; k++) {

					String search = (String) hash[k][0];
					for (int m = 0; m < metadata.size(); m++) {
						if (metadata.get(m)[1].equalsIgnoreCase(search)) {
							String type = metadata.get(m)[2].toLowerCase();
							try {

								if (type.contains("integer")) {
									tuples.get(rowNum).obj.set(m, (Integer) hash[k][1]);
								} else if (type.contains("double")) {
//call update indices b el tuple eladeem
									tuples.get(rowNum).obj.set(m, (Double) hash[k][1]);

								} else if (type.contains("string")) {
									tuples.get(rowNum).obj.set(m, (String) hash[k][1]);

								} else if (type.contains("boolean")) {
									tuples.get(rowNum).obj.set(m, (Boolean) hash[k][1]);

								} else if (type.contains("polygon")) {
									tuples.get(rowNum).obj.set(m, (Polygon) hash[k][1]);

								} else if (type.contains("date")) {
									tuples.get(rowNum).obj.set(m, (Date) hash[k][1]);

								}

							} catch (ClassCastException e) {
								throw new DBAppException("You entered a wrong type");
							}
						}

					}
				}

				ObjectOutputStream output12 = new ObjectOutputStream(
						new FileOutputStream("data\\" + T.TableName + pNum + ".class"));
				output12.writeObject(tuples);// rewrites the pages back
				output12.flush();
				output12.close();
			}
//get page and row numbers........Tuple
//update indices

		}
//NORMAL BINARY SEARCH THRO PAGES		
		boolean done = false;
		for (int i = 0; i < T.Pages.size() && done == false && indexOnClstr == false; i++) {
			System.out.println("NOT HERE");
			Vector<Integer> allpos = new Vector<Integer>();
			ObjectInputStream inputpage = new ObjectInputStream(
					new FileInputStream("data\\" + T.TableName + T.Pages.get(i).number + ".class")); // get
// the
// table's
// file
			Vector<Tuple> tuples = (Vector<Tuple>) inputpage.readObject(); // deserialize
// the
// table's
// object
			inputpage.close();

			String dateString = "";
			int pos = binarySearch(tuples, 0, tuples.size() - 1, strKey, keytype);
			allpos.add(pos);
			if (pos != -1) {
// String dateClustringkey = strKey;
//handling duplicates
//going forward starting from the pos where i found my clustering key using binary Search until I hit a different key
//when I find one different key this means that there are no more duplicates, thus I am "done" searching through all pages
				for (int forw = pos + 1; forw < tuples.size(); forw++) {
					String c = "";
					if (isPolygon) {
						int x[] = ((Polygon) ((tuples.get(forw)).obj.get(0))).xpoints;
						int arr[] = ((Polygon) ((tuples.get(forw)).obj.get(0))).ypoints;
						c = makeIntoString(x, arr);
					} else if (keytype.contains("date")) {
						String strClustringkey = strKey;

						String[] s = strClustringkey.split("-");
//handling the toString of dates as the input clustering key is in a specific format
						Date d = new Date(Integer.parseInt(s[0]), Integer.parseInt(s[1]), Integer.parseInt(s[2]));
						Date d2 = (Date) ((tuples.get(forw)).obj.get(0));
						c = d2.toString();
						dateString = d.toString();

					}

					else {
						c = ((tuples.get(forw)).obj.get(0)).toString();
						System.out.println("The c is:" + c + " Thestrkey is:" + strKey);
					}
					if (c.equals(strKey) || c.equals(dateString)) {
						allpos.add(forw);

					} else {
						done = true;
						break;
					}

				}
				for (int back = pos - 1; back >= 0; back--) {
//handling duplicates
//going back starting from the pos where i found my clustering key using binary Search until I hit a different key
					String c = "";
					if (isPolygon) {
						int x[] = ((Polygon) ((tuples.get(back)).obj.get(0))).xpoints;
						int arr[] = ((Polygon) ((tuples.get(back)).obj.get(0))).ypoints;
						c = makeIntoString(x, arr);
					} else if (keytype.contains("date")) {
						String strClustringkey = strKey;

						String[] s = strClustringkey.split("-");

						Date d = new Date(Integer.parseInt(s[0]), Integer.parseInt(s[1]), Integer.parseInt(s[2]));
						Date d2 = (Date) ((tuples.get(back)).obj.get(0));
						c = d2.toString();
						dateString = d.toString();

					}

					else {
						c = ((tuples.get(back)).obj.get(0)).toString();
					}
					if (c.equals(strKey) || c.equals(dateString)) {
						allpos.add(back);

					} else {

						break;
					}

				}
				replaced = true;

				System.out.println("i" + T.Pages.get(i).number);

				for (int hadto = 0; hadto < hash.length; hadto++) {
					if (keycol.equalsIgnoreCase((String) hash[hadto][0])) {

						throw new DBAppException("please remove the clustering key from cols to be updated.");

					}
				}
				for (int go = 0; go < allpos.size(); go++) {
					pos = allpos.elementAt(go);
					tuples.get(pos).obj.set(tuples.get(pos).obj.size() - 1,
							new SimpleDateFormat("yyyy-MM-dd:HH-mm-ss").format(new Date()));
					Pointer po = new Pointer(strTableName, T.Pages.get(i).number, pos);
					System.out.println(po + "  " + pos + " this what went through update");
					updateIndices(hash, T, po, tuples.get(pos));
					for (int k = 0; k < hash.length; k++) {

						String search = (String) hash[k][0];

						for (int m = 0; m < metadata.size(); m++) {
							if (metadata.get(m)[1].equalsIgnoreCase(search)) {
								String type = metadata.get(m)[2].toLowerCase();
								try {

									if (type.contains("integer")) {
										tuples.get(pos).obj.set(m, (Integer) hash[k][1]);
									} else if (type.contains("double")) {
//call update indices b el tuple eladeem
										tuples.get(pos).obj.set(m, (Double) hash[k][1]);

									} else if (type.contains("string")) {
										tuples.get(pos).obj.set(m, (String) hash[k][1]);

									} else if (type.contains("boolean")) {
										tuples.get(pos).obj.set(m, (Boolean) hash[k][1]);

									} else if (type.contains("polygon")) {
										tuples.get(pos).obj.set(m, (Polygon) hash[k][1]);

									} else if (type.contains("date")) {
										tuples.get(pos).obj.set(m, (Date) hash[k][1]);

									}
								} catch (ClassCastException e) {
									System.out.println("You entered a wrong type");
								}
							}

						}
					}

					ObjectOutputStream output = new ObjectOutputStream(
							new FileOutputStream("data\\" + T.TableName + T.Pages.get(i).number + ".class"));
					output.writeObject(tuples);// rewrites the pages back
					output.flush();
					output.close();

// break;
				}
			}

		}

		if (replaced == false) {
			throw new DBAppException("This key was never found");
		}

	}

	private overFlowPointers searchBtreeHelper(String strKey, String keytype, BTree b) {
		if (keytype.contains("integer")) {
			int d = Integer.parseInt(strKey);
			return b.search(d);
		} else if (keytype.contains("double")) {
			Double d = Double.parseDouble(strKey);
			return b.search(d);
		} else if (keytype.contains("boolean")) {
			Boolean d = Boolean.parseBoolean(strKey);
			return b.search(d);
//} else if (keytype.contains("polygon")) {
////	Poly p=new Poly();	
//Polygon d=makePolygon(strKey);//d
////p.p=d;
//return b.search(d);
		} else if (keytype.contains("date")) {
			String strClustringkey = strKey;
			String[] s = strClustringkey.split("-");
			Date d = new Date(Integer.parseInt(s[0]), Integer.parseInt(s[1]), Integer.parseInt(s[2]));
			return b.search(d);
		} else {

			String d = strKey;
			return b.search(d);
		}

	}

	public static void main(String[] args) throws Exception {

		DBApp p = new DBApp();
		p.init();
//		Hashtable<String, String> var = new Hashtable<String, String>();
//		var.put("Name", "java.lang.String");
//		var.put("ID", "java.lang.Integer");
//		var.put("gpa", "java.lang.Double");
//		var.put("Shape", "java.awt.Polygon");
//		var.put("date","java.util.Date");
//		var.put("bool","java.lang.Boolean");
//		p.createTable("final", "Shape", var);

		// p.createRTreeIndex("finaltableee", "shape").printTree();
//		p.createBTreeIndex("finaltable", "ID").printTree();
//		p.createBTreeIndex("finaltable", "gpa").printTree();
//		p.createBTreeIndex("finaltable", "date").printTree();
//		p.createBTreeIndex("finaltable", "bool").printTree();
////		p.createBTreeIndex("ST", "ID").printTree();
//		p.createBTreeIndex("dot", "gpa").printTree();
//		 p.createRTreeIndex("pT", "Shape").rprintTree();
//		p.createBTreeIndex("dT", "date").printTree();
		// p.createBTreeIndex("bt", "bool").printTree();
//		int [] x= {50,50,30};
//		int [] y= {0,2,10};
//
//
//		Table t = p.loadTable("dont");
//		t.printTable();
//		 Hashtable<String, Object> var = new Hashtable<String, Object>();
//
//var.put("Name", "new");
//////// var.put("bool",false);
//var.put("ID", new Integer(4));
////var.put("ID2", new Integer(200));
////var.put("gpa", new Double(8.8));
//		// var.put("date", new Date(1998,7,3));
//// var.put("Shape", new Polygon(x,y,x.length));
//p.insertIntoTable("dont", var);
////p.insertIntoTable("dot", var);
//

		// p.updateTable("dont", "4", var);
	//	 p.createBTreeIndex("yarabfinalll", "name").printTree();
//		 p.createRTreeIndex("yarabfinal", "shape").printTree();
//		 
//		 p.createRTreeIndex("notpol", "shape").printTree();
		// p.updateTable("dot", "8.8", var);
////		System.out.println("NEEEWWW");
//		Table ts2 = p.loadTable("finaltableee");
//		Table ts3 = p.loadTable("finaltablee");
//		// t2.printTable();
//////		 
//////
//////		 
//		System.out.println("PRINTING TREE");
//		RTree b = loadRTree(ts2.TableName, "shape");
//		b.rprintTree();
//
//		System.out.println("PRINTING TREE");
//		RTree b1 = loadRTree(ts3.TableName, "shape");
//		b1.rprintTree();

		// System.out.println("ROOOT" + b.root.getClass());
//		System.out.println();
//		BTree b2 = LoadBtree(t2.TableName, "gpa");
//		b2.printTree();
//		
//		System.out.println();
//		BTree b3=LoadBtree(t2.TableName, "bool");
//		b3.printTree();
//		
//		
//		System.out.println();
//		BTree b4=LoadBtree(t2.TableName, "date");
//		b4.printTree();
//		
//		System.out.println();
//		BTree b5=LoadBtree(t2.TableName, "ID");
//		b5.printTree();

//		System.out.println();
//		
//		RTree b6=loadRTree(t2.TableName, "Shape");
//		b6.rprintTree();
//		System.out.println();

//////////////////////////////Yoo Testing////////////////////////

		Polygon p1 = new Polygon();
		Poly ps1 = new Poly();

		p1.addPoint(0, 0);
		p1.addPoint(5, 0);
		p1.addPoint(5, 5);
		p1.addPoint(0, 5);
		ps1.p = p1;

		Polygon p2 = new Polygon();
		Poly ps2 = new Poly();

		p2.addPoint(0, 0);
		p2.addPoint(10, 0);
		p2.addPoint(10, 5);
		p2.addPoint(0, 5);

		ps2.p = p2;

		Polygon p3 = new Polygon();
		Poly ps3 = new Poly();

		p3.addPoint(10, 0);
		p3.addPoint(30, 0);
		p3.addPoint(30, 10);
		p3.addPoint(10, 10);

		ps3.p = p3;

		Polygon p4 = new Polygon();
		Poly ps4 = new Poly();
		p4.addPoint(0, 0);
		p4.addPoint(10, 0);
		p4.addPoint(10, 10);
		p4.addPoint(0, 10);

		ps4.p = p4;

		Polygon p5 = new Polygon();
		Poly ps5 = new Poly();

		p5.addPoint(10, 10);
		p5.addPoint(20, 10);
		p5.addPoint(20, 20);
		p5.addPoint(10, 20);

		ps5.p = p5;
		
		
		Polygon p6 = new Polygon();
		Poly ps6 = new Poly();

		p6.addPoint(1, 1);
		p6.addPoint(11, 1);
		p6.addPoint(11, 6);
		p6.addPoint(1, 6);

		ps6.p = p6;
		
		
		

////////		 
//////
//////		 
//		System.out.println("PRINTING TREE");
		// BTree b = LoadBtree(ts.TableName, "Name");
		// b.printTree();

		int x2[] = { 3, 6, 1 };
		int y2[] = { 5, 1, 1 };// area =20
////		 var.put("Name", "da hob hayaty");
////		 var.put("ID", 32);
////		 var.put("Height", 18.5);
//		 var.put("Shape", new Polygon(x2, y2, x2.length));
		
		int []x7= {3,2,1,3};
		 int []y7= {5,1,1,1};
//		 
//		 Table t1 = p.loadTable("pi");
//		 t1.printTable();		 
//		 Polygon polygon=new Polygon(x7,y7,x7.length);
		
		 
		 Table ts1 = p.loadTable("final");
			ts1.printTable();

		 
		Hashtable<String, Object> var = new Hashtable<String, Object>();
		var.put("Name", "skittles");
//		var.put("ID", 4);
//		var.put("gpa", 6.8);
//		var.put("date", new Date(1203, 8, 7));
	//	var.put("Shape", p2);
//
//		var.put("bool", true);
		p.updateTable("final", "(0,0),(10,0),(10,5),(0,5)", var);
	//	p.insertIntoTable("final", var);
////////////	
//		var.clear();
//		var.put("Name", "gded");
//		var.put("ID", 140);
//		var.put("date", new Date(2023, 8, 1));
//		var.put("bool", false);
//		var.put("gpa", 38.8);
//		var.put("Shape", p2); // 8
////////		 var.put("date", new Date(1997,7,15));
//////////		
//		p.insertIntoTable("yarabfinalll", var);
//////////////		
//		var.clear();
//		var.put("Name", "hena");
//		var.put("ID", 245);
//		var.put("date", new Date(3015, 8, 2));
//		var.put("bool", true);
//		var.put("gpa", 8.6);
//		var.put("Shape", p2);
////		 
////////		 var.put("date", new Date(1997,7,15));
////////////		
//		p.insertIntoTable("yarabfinalll", var);
//////		//
//		var.clear();
//		var.put("Name", "thetriangle");
//		var.put("ID", 100);
//		var.put("date", new Date(2004, 2, 1));
//		var.put("bool", false);
//		var.put("gpa", 6.2);
//		var.put("Shape", p3);
////		 
//////		 var.put("date", new Date(1997,7,15));
////////		
//		p.insertIntoTable("yarabfinal", var);
//
//		var.clear();
//		var.put("Name", "zoon");
//		var.put("ID", 490);
//		var.put("date", new Date(2041, 8, 1));
//		var.put("bool", true);
//		var.put("gpa", 53.5);
//		var.put("Shape", p1);
//////		 
////////		 var.put("date", new Date(1997,7,15));
//////////		
//		p.insertIntoTable("yarabfinal", var);
//
//////		 
//		var.clear();
//		var.put("Name", "lool");
//		var.put("ID", 3);
//		var.put("date", new Date(2021, 9, 1));
//		var.put("bool", false);
//		var.put("gpa", 1.5);
//		var.put("Shape", p4);
////		 
//////		 var.put("date", new Date(1997,7,15));
////////		
//		p.insertIntoTable("yarabfinal", var);

//////		 
//////		 
		Table ts = p.loadTable("final");
		ts.printTable();

//		Table ts1 = p.loadTable("finaltablee");
//		ts1.printTable();
//
//		Table ts33 = p.loadTable("notpol");
//		ts33.printTable();
////			
//
		// Hashtable<String, Object> var = new Hashtable<String, Object>();
//////////			int[] x = { 3, 2, 1,3 }; 
//////////			int[] y = { 5, 1, 1,1 };//Area =8
//		 var.put("name", "mar");
		// var.put("name","yomyom");
////			//var.put("ID", new Integer(700));
////////			//var.put("ID2", new Integer(200));
////		var.put("ID", new Integer(3));
////////				var.put("date", new Date(1998,7,3));
////////			// var.put("Shape", new Polygon(x,y,x.length));	
////////			p.updateTable("finaltab","(0,0),(5,0),(5,5),(0,5)" , var);
////////			 Table ts1 = p.loadTable("finaltab");
////////				ts1.printTable();
//////
//////		// var.put("Date", new Date(1997, 7, 1));
//////		// var.put("GPA", new Double(1.7));
//////
		// p.updateTable("finaltablee", "(10,0),(30,0),(30,10),(10,10)", var);
////
////		// p.insertIntoTable("theTable", var);
////		//
		// p.deleteFromTable("finaltableee", var);
		//
		//

//		 
//		  ts = p.loadTable("yarabfinalll");
//			ts.printTable();
//		 Table t1 = p.loadTable("dont");
//		 t1.printTable();
//		 
//		BTree b= LoadBtree("dont", "Name");
//		b.printTree();
//var.clear();

//var.put("Name", "nee");
//var.put("ID", 7);
//p.insertIntoTable("dont", var);
//var.clear();

//var.put("Name", "nee");

//p.insertIntoTable("dont", var);
////var.clear();
//p.deleteFromTable("dont", var);

		// p.insertIntoTable("dont", var);
//		 Table t2 = p.loadTable("dont");
//		 t2.printTable();
//			BTree b2= LoadBtree("dont", "Name");
//			b2.printTree();
//			p.GetMeallRecordsPageCheck("dont", "Name", "Martha a7la");
//		Hashtable<String, String> var = new Hashtable<String, String>();
//		var.put("Name", "java.lang.String");
//		var.put("ID", "java.lang.Integer");
//		var.put("Height", "java.lang.Double");
//		var.put("Shape", "java.awt.Polygon");
////		var.put("date","java.util.Date");
////		
//		p.createTable("selecttrialtwo", "name", var);
//
//		List<String> names = new LinkedList<>();
//		names.add("Rams");[
//		names.add("Posa");
//		names.add("Chinni");

		// Getting Iterator
//		Iterator<String> namesIterator = names.iterator();
//
//		// Traversing elements
//		while (namesIterator.hasNext()) {
//			System.out.println(namesIterator.next());
//
//		}

//		String y = "Yo";
//		String f = "Ye";
//		Object t1 = "yo";
		// System.out.print((t.compareTo((Object)f));

//		 System.out.println("TOOOOOOOOOOOOOOOOO");

//		System.out.println("HEREE");
//
//		p.LoadBtree("finaltab", "id").printTree();
//		p.GetMeallRecordsPageCheck("finaltab", "id", 3);
//
//		System.out.println("HEREE");

////
		SQLTerm t = new SQLTerm("yarabfinalll", "id", ">", 150);
		SQLTerm t3 = new SQLTerm("yarabfinalll", "name", "!=", "hena");

		SQLTerm t4 = new SQLTerm("yarabfinal", "bool", "=", true);
		// SQLTerm t4 = new SQLTerm("finaltable", "id", "<=", 120);
//
		SQLTerm[] array = new SQLTerm[2];
		array[0] = t;

		array[1] = t3;

		// array[2] = t4;
		// array[3] = t4;
		String[] op = { "and"};

//
//		Iterator resultSet = p.selectFromTable(array, op);
//
//		System.out.println("Select Answer");
//		while (resultSet.hasNext()) {
//
//			System.out.println(resultSet.next());
//
//		}

//		Vector<Tuple> vector1 = new Vector();
//		Vector<Tuple> vector2 = new Vector();
//
//		Tuple tuple1 = new Tuple();
//		Tuple tuple2 = new Tuple();
//		Tuple tuple3 = new Tuple();
//		Tuple tuple4 = new Tuple();
//
//		Tuple[] array1 = new Tuple[3];
//		Tuple[] array2 = new Tuple[3];
//
//		Vector<Object> obj1 = new Vector();
//		Vector<Object> obj2 = new Vector();
//		Vector<Object> obj3 = new Vector();
//		Vector<Object> obj4 = new Vector();
//
//		obj1.add("yoo");
//		obj1.add(50);
////
//		obj2.add("foo");
//		obj2.add(50);
//
//		obj3.add("zoz");
//		obj3.add(150);
//
//		obj4.add("noo");
//		obj4.add(200);
//
//		tuple1.obj = obj1;
//		tuple2.obj = obj2;
//		tuple3.obj = obj3;
//		tuple4.obj = obj4;

//		System.out.println(tuple1.leehhh(tuple2));
//
//		vector1.add(tuple1);
//		vector1.add(tuple2);
//		vector1.add(tuple3);
//		vector1.add(tuple4);
//
//		Vector<Tuple> result2 = new Vector();
//		 Tuple[] result2 = new Tuple[10];
//		result2 = binarySearchSelect(vector1, 0, 3, 130, 1, "<");
//
////		Tuple[] result = new Tuple[10];
////		result = anding(h1, h2);
//
//		for (int i = 0; i < result2.size(); i++) {
//
//			System.out.println(result2.get(i).obj);
//			System.out.println(i);
//			// System.out.println(result[i].obj );
//
//		}
//		array1[0] = tuple3;
//		array1[1] = tuple4;
//		array1[2] = tuple3;
//
//		array2[0] = tuple4;
//		array2[1] = tuple2;
//		array2[2] = tuple1;
//
//		result2 = xoring(array1, array2);
//
//		for (int i = 0; i < result2.size(); i++) {
//
//			System.out.println(result2.get(i).obj);
//			System.out.println(i);
//		}

//			
//		}

//		Hashtable<String, String> var = new Hashtable<String, String>();

//		var.put("Name", "java.lang.String");
//		var.put("ID", "java.lang.Integer");
//		var.put("Shape", "java.awt.Polygon");
//
//		p.createTable("pi", "Shape", var);

//		p.createBTreeIndex("dot", "Name").printTree();
//		p.createBTreeIndex("ST", "ID").printTree();
//		p.createBTreeIndex("dot", "gpa").printTree();
//		p.createRTreeIndex("pi", "Shape").rprintTree();
//		p.createBTreeIndex("dT", "date").printTree();
//		p.createBTreeIndex("bt", "bool").printTree();

//	Hashtable<String, Object> var = new Hashtable<String, Object>();
//		int[] x6 =  {3,2,1,3};
//		int[] y6 =  {5,1,1,1};//Area 14
//		var.put("Name", "dof3a");
//		//var.put("ID", new Integer(30));
//		var.put("Shape", new Polygon(x6, y6, x6.length));
//		// p.deleteFromTable("pi", var);
//		p.insertIntoTable("pi", var);
//		var.clear();
////	
//		int[] x = {8,7,9,9 };
//		int[] y = {8,2,9,9 };// Area 14
//	//	var.put("Name", "besm");
////		var.put("ID", new Integer(50));
//		var.put("Shape", new Polygon(x, y, x.length));
//		p.insertIntoTable("pi", var);
//		var.clear();
//		
//		int x2[]= {3,6,1};
//		int y2[]= {5,1,1};//area =20
//		 var.put("Name", "da hob hayaty");
////		 var.put("ID", 32);
////		 var.put("Height", 18.5);
//		 var.put("Shape", new Polygon(x2, y2, x2.length));
////		 var.put("gpa", 50.0);
//
////		 var.put("date", new Date(2020,2,1));
//
//		 p.insertIntoTable("pi", var);
//		 var.clear();
//		
//		///// BEFORE THE NEW SELECT METHOD
//
//		 
//		 
//		 int []x7= {3,2,1,3};
//		 int []y7= {5,1,1,1};
//		 
//		 Table t1 = p.loadTable("pi");
//		 t1.printTable();		 
//		 Polygon polygon=new Polygon(x7,y7,x7.length);
//		 Poly poly=new Poly();
//		 poly.p=polygon;
//		RTree b= loadRTree("pi", "Shape");
//		b.rprintTree();
//		p.GetMeallR("pi", "Shape",poly );
		
		
		//THIS IS THE FINAL ONEE
	}

}
