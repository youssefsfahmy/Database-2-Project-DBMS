package Sharks;

import java.awt.Dimension;
import java.awt.Polygon;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Vector;

public class Poly implements Comparable<Poly>, Serializable {
	Polygon p;

	public Poly() {
		this.p = new Polygon();
	}

	@Override
	public int compareTo(Poly p) {

		Polygon p1 = new Polygon();
		p1 = p.p;
		Polygon p2 = new Polygon();
		p2 = this.p;
		Dimension dim = p1.getBounds().getSize();
		int Areap1 = dim.width * dim.height;
		Dimension dim2 = p2.getBounds().getSize();
		int Areap2 = dim2.width * dim2.height;

		return Areap2 - Areap1;
	}

	public int compareToSelectRTree(Poly p, String operator) {

		Polygon p1 = new Polygon();
		p1 = p.p;
		Polygon p2 = new Polygon();
		p2 = this.p;
		Dimension dim = p1.getBounds().getSize();
		int Areap1 = dim.width * dim.height;
		Dimension dim2 = p2.getBounds().getSize();
		int Areap2 = dim2.width * dim2.height;

		return Areap2 - Areap1;

//		if (operator.equals("<") || operator.equals(">")) {
//
//			return Areap2 - Areap1;
//		}
//
//		else if (operator.equals("=")) {
//
//			if (Arrays.equals(this.p.xpoints, p.p.xpoints) && Arrays.equals(this.p.ypoints, p.p.ypoints)) {
//
//				return 0;
//			} else {
//				return -1;
//			}
//		}
//
//		else if (operator.equals("!=")) {
//
//			if (Arrays.equals(this.p.xpoints, p.p.xpoints) && Arrays.equals(this.p.ypoints, p.p.ypoints)) {
//				
//				System.out.println("zeroo");
//
//				return 0;
//			} else {
//				
//				System.out.println("one");
//				return -1;
//			}
//		}
//
//		else if (operator.equals("<=") || operator.equals(">=")) {
//
//			if (Arrays.equals(this.p.xpoints, p.p.xpoints) && Arrays.equals(this.p.ypoints, p.p.ypoints)) {
//
//				return 0;
//			} else {
//
//				return Areap2 - Areap1;
//
//			}
//
//		}
//
//
//		return Areap2 - Areap1;
	}

	public Vector<Tuple> compareToPOLY(Vector<Tuple> tuples, String operator, int colpos, Poly p) {
		
		Vector<Tuple> t = new Vector();
		
		if(operator.equals("<") || operator.equals("<=")|| operator.equals(">")|| operator.equals(">=")) {
			
			t=tuples;
			return t;
		}
		
		

		for (int i = 0; i < tuples.size(); i++) {

			Polygon p1 = new Polygon();
			p1 = p.p;
			Polygon p3 = new Polygon();
			Poly p2 = new Poly();

			p3 = (Polygon) tuples.get(i).obj.get(colpos);
			p2.p = p3;

			if (operator.equals("=")) {

				//System.out.println("i" + i);
				//System.out.println(
					//	"ndc" + (Arrays.equals(p.p.xpoints, p2.p.xpoints) && Arrays.equals(p.p.ypoints, p2.p.ypoints)));

				if (Arrays.equals(p.p.xpoints, p2.p.xpoints) && Arrays.equals(p.p.ypoints, p2.p.ypoints)) {
					
					t.add(tuples.get(i));

				} 
			}

			else if (operator.equals("!=")) {

				

				if (Arrays.equals(p1.xpoints, p3.xpoints) && Arrays.equals(p1.ypoints, p3.ypoints)) {

				

				} else {
					t.add(tuples.get(i));
					

				}
			

		}

	}return t;

	}

	public boolean compareCoordinates(Poly p) {
		if (p.compareTo(this) != 0) {
			return false;
		}

		Polygon p1 = new Polygon();
		p1 = p.p;
		Polygon p2 = new Polygon();
		p2 = this.p;

		int[] xpoints1 = p1.xpoints;
		int[] ypoints1 = p1.ypoints;

		int[] xpoints2 = p2.xpoints;
		int[] ypoints2 = p2.ypoints;

		for (int i = 0; i < xpoints1.length; i++) {
			int x1 = xpoints1[i];
			int y1 = ypoints1[i];
			boolean found = false;
			for (int j = 0; j < xpoints2.length; j++) {

				int x2 = xpoints2[j];
				int y2 = ypoints2[j];

				if (x1 == x2 && y1 == y2) {
					found = true;
				}
			}
			if (!found) {
				return false;
			}
		}
		return true;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Poly ps = new Poly();
		Polygon p1 = new Polygon();
		p1.addPoint(10, 10);
		p1.addPoint(20, 10);
		p1.addPoint(20, 20);

		ps.p = p1;

		Poly ps1 = new Poly();
		Polygon p2 = new Polygon();
		p2.addPoint(0, 0);
		p2.addPoint(10, 0);
		p2.addPoint(10, 10);
		ps1.p = p1;

	}

}
