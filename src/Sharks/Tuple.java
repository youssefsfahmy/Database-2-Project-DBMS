package Sharks;

import java.awt.Polygon;
import java.io.Serializable;
import java.sql.Date;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Vector;

public class Tuple implements Serializable, Comparable<Tuple> {
	int rowNumber;// 18
	Vector<Object> obj = new Vector();
	int pageNo;

	public String toString() {
		String out = "[";

		for (int i = 0; i < obj.size(); i++) {
			String m = "" + obj.get(i).toString();
			if (obj.get(i).toString().contains("Polygon")) {
				try {
					m = makeIntoString(((Polygon) obj.get(i)).xpoints, ((Polygon) obj.get(i)).ypoints);
				} catch (DBAppException e) {
					// TODO Auto-generated catch block
					System.out.println("Invalid INPUT");
				}
			}

			out = out + m + "  ,  ";
		}
		return out + "]";

	}

	public int compareTo(Tuple t2) {
		Object O1 = this.obj.get(0);
		Object O2 = t2.obj.get(0);

		if (O1 instanceof java.lang.String) {

			String s1 = (String) O1;
			String s2 = (String) O2;

			s1 = s1.toLowerCase();
			s2 = s2.toLowerCase();
			return s1.compareTo(s2);

		}
		if (O1 instanceof java.lang.Integer) {
			int i1 = (int) O1;
			int i2 = (int) O2;
			return i1 - i2;

		}

		if (O1 instanceof java.lang.Boolean) {
			Boolean b1 = (Boolean) O1;
			Boolean b2 = (Boolean) O2;

			return Boolean.compare(b1, b2);
		}
		if (O1 instanceof java.lang.Double) {
			Double do1 = (Double) O1;
			Double do2 = (Double) O2;
			return do1.compareTo(do2);

		}
		if (O1 instanceof java.util.Date) {

			// System.out.println(O1);

			// String da1=new SimpleDateFormat("yyyy-MM-dd:HH-mm-ss").format(new Date());

			java.util.Date da1 = (java.util.Date) O1;
			java.util.Date da2 = (java.util.Date) O2;
			return da1.compareTo(da2);

		}

		if (O1 instanceof java.awt.Polygon) {

			Poly p1 = new Poly();
			p1.p = (Polygon) O1;
			Poly p2 = new Poly();
			p2.p = (Polygon) O2;

//			Poly p1=(Poly)O1;
//			Poly p2=(Poly)O2;
			return p1.compareTo(p2);

		}

		return 0;
	}

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

	public int compareToSelect(Object O2, int pos, String operator) throws DBAppException {

		Object O1 = this.obj.get(pos);

		try {

			if (O1 instanceof java.lang.String) {

				String s1 = (String) O1;
				String s2 = (String) O2;
				return s1.toLowerCase().compareTo(s2.toLowerCase());

			}
			if (O1 instanceof java.lang.Integer) {
				int i1 = (int) O1;
				int i2 = (int) O2;
				return i1 - i2;

			}
			if (O1 instanceof java.lang.Double) {
				Double do1 = (Double) O1;
				Double do2 = (Double) O2;
				return do1.compareTo(do2);

			}
			if (O1 instanceof java.util.Date) {

				java.util.Date da1 = (java.util.Date) O1;
				java.util.Date da2 = (java.util.Date) O2;
				return da1.compareTo(da2);

			}
			if (O1 instanceof java.lang.Boolean) {
				Boolean b1 = (Boolean) O1;
				Boolean b2 = (Boolean) O2;

				return Boolean.compare(b1, b2);
			}

			if (O1 instanceof java.awt.Polygon) {

				if (operator.equals("<") || operator.equals(">")) {

					Poly p1 = new Poly();
					p1.p = (Polygon) O1;
					Poly p2 = new Poly();
					p2.p = (Polygon) O2;

					return p1.compareTo(p2);
				}

				else if (operator.equals("=")) {

					Poly p1 = new Poly();
					p1.p = (Polygon) O1;
					Poly p2 = new Poly();
					p2.p = (Polygon) O2;

					if (Arrays.equals(p1.p.xpoints, p2.p.xpoints) && Arrays.equals(p1.p.ypoints, p2.p.ypoints)) {

						return 0;
					} else {
						return -1;
					}
				}

				else if (operator.equals("!=")) {

					Poly p1 = new Poly();
					p1.p = (Polygon) O1;
					Poly p2 = new Poly();
					p2.p = (Polygon) O2;

					if (Arrays.equals(p1.p.xpoints, p2.p.xpoints) && Arrays.equals(p1.p.ypoints, p2.p.ypoints)) {

						return 0;
					} else {
						return -1;
					}
				}

				else if (operator.equals("<=") || operator.equals(">=")) {

					Poly p1 = new Poly();
					p1.p = (Polygon) O1;
					Poly p2 = new Poly();
					p2.p = (Polygon) O2;

					if (Arrays.equals(p1.p.xpoints, p2.p.xpoints) && Arrays.equals(p1.p.ypoints, p2.p.ypoints)) {

						return 0;
					} else {

						return p1.compareTo(p2);

					}

				}

			}

		} catch (Exception e) {
			throw new DBAppException("WRONG TYPE");

		}

		return 0;
	}

	public boolean leehhh(Tuple t2) {

		int l = t2.obj.size();

		boolean check = true;

		for (int x = 0; x < l - 1; x++) {

			if (this.obj.get(x) instanceof Polygon) {

				Poly p1 = new Poly();
				p1.p = (Polygon) this.obj.get(x);
				Poly p2 = new Poly();
				p2.p = (Polygon) t2.obj.get(x);

				if (!(Arrays.equals(p1.p.xpoints, p2.p.xpoints) && Arrays.equals(p1.p.ypoints, p2.p.ypoints))) {
					check = false;
					break;

				}

			} else {
				if (!(this.obj.get(x).equals(t2.obj.get(x)))) {

					check = false;
					break;

				}
			}

		}
		return check;

	}

	public static void main(String[] args) throws ParseException, DBAppException {

		Tuple tuple1 = new Tuple();
		Tuple tuple2 = new Tuple();

		Vector<Object> obj1 = new Vector();
		Vector<Object> obj2 = new Vector();

		Polygon p = new Polygon();

		p.addPoint(10, 10);
		p.addPoint(20, 10);
		p.addPoint(20, 20);
		p.addPoint(10, 20);

		obj1.add("martha");
		obj1.add(50);
		obj1.add(p);
		obj1.add(100.0);

		Tuple T100 = new Tuple();
		Tuple T200 = new Tuple();
		T100.obj = obj1;
		T200.obj = obj1;

		// System.out.println("YOOO" + T100.leehhh(T200));
//
		obj2.add("yoo");
		obj2.add(50);

		tuple1.obj = obj1;
		tuple2.obj = obj2;

		System.out.println(tuple1.leehhh(tuple2));

		Polygon p1 = new Polygon();

		p1.addPoint(20, 20);
		p1.addPoint(30, 20);
		p1.addPoint(30, 30);
		p1.addPoint(20, 30);

		Tuple t1 = new Tuple();
		t1.obj.add(p);

		System.out.println(t1.compareToSelect(p1, 0, "!="));

		// Tuple t1 = new Tuple();
		t1.obj.add(2);
		Tuple t2 = new Tuple();
		t2.obj.add(1);
		Tuple t3 = new Tuple();
		t3.obj.add(1);
		Tuple t4 = new Tuple();
		t4.obj.add(1);
		Tuple t5 = new Tuple();
		t5.obj.add(1);
		Tuple t6 = new Tuple();
		t6.obj.add(1);

		Tuple ts = new Tuple();
		ts.obj.add(1);

		Vector<Tuple> vectorArr = new Vector<Tuple>();

		vectorArr.add(t1);
		vectorArr.add(t2);
		vectorArr.add(t3);
		vectorArr.add(t4);
		vectorArr.add(t5);
		vectorArr.add(t6);

		int tupleIndex = Collections.binarySearch(vectorArr, ts);

		System.out.println(tupleIndex);

		System.out.println(p.equals(p));

	}

}
