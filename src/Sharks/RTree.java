package Sharks;

import java.awt.Polygon;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Vector;

public class RTree<TKey extends Comparable<TKey>, TValue> implements java.io.Serializable {
	BTreeNode<TKey> root;

	public RTree() {

		this.root = new BTreeLeafNode<TKey, TValue>();
	}

	/**
	 * Insert a new key and its associated value into the B+ tree.
	 */
	public void insert(TKey key, TValue value) {
		BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);
		leaf.insertKey(key, value);

		if (leaf.isOverflow()) {
			BTreeNode<TKey> n = leaf.dealOverflow();
			if (n != null)
				this.root = n;
		}
	}

	/**
	 * Search a key value on the tree and return its associated value.
	 */
	public overFlowPointers search(TKey key) {
		BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);

		int index = leaf.search(key);
		return (index == -1) ? null : leaf.getValue(index);
	}

	/**
	 * Delete an entire key and its associated value from the tree.
	 */
	public void delete(TKey key) {
		BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);

		if (leaf.delete(key) && leaf.isUnderflow()) {
			BTreeNode<TKey> n = leaf.dealUnderflow();
			if (n != null)
				this.root = n;
		}
	}

	/**
	 * Delete a specific Value within an Index
	 * 
	 * @throws DBAppException
	 */
	public void deleteValue(TKey key, TValue value) throws DBAppException {
		overFlowPointers o = this.search(key);
		Pointer Pvalue = (Pointer) value;
		int x = -1;
		for (int i = 0; i < o.values.size(); i++) {
			Pointer p1 = (Pointer) o.values.get(i);
			if (p1.compareTo(Pvalue) == 0) {
				x = i;
				break;
			}
		}

		if (x != -1) {
			if (o.isOverFlow()) {
				o.values.remove(x);
			} else {
				this.delete(key);
			}
		} else {
			throw new DBAppException("This pointer doesn't exist within this index");

		}
	}

	/**
	 * Search the leaf node which should contain the specified key
	 */
	@SuppressWarnings("unchecked")
	public BTreeLeafNode<TKey, TValue> findLeafNodeShouldContainKey(TKey key) {
		BTreeNode<TKey> node = this.root;
		while (node.getNodeType() == TreeNodeType.InnerNode) {
			node = ((BTreeInnerNode<TKey>) node).getChild(node.search(key));
		}

		return (BTreeLeafNode<TKey, TValue>) node;
	}

	/**
	 * update a specific value within an index
	 * 
	 * @throws DBAppException
	 */
	@SuppressWarnings("unused")
	private void updatePointerValue(TKey key, TValue value, TValue newValue) throws DBAppException {
		this.search(key).replace(value, newValue);
		;
	}

	public void saveRTree(String strTableName, String strColName) throws IOException {

		ObjectOutputStream output = new ObjectOutputStream(
				new FileOutputStream("data\\" + "RTree" + strTableName + "On" + strColName + ".class"));
		output.writeObject(this);// rewrites the pages back
		output.flush();
		output.close();

	}

	public void printTree() {
		this.root.print();
	}

	public void rprintTree() {
		this.root.rprint();
	}

	public ArrayList<Pointer> getLeafPointers() {
		ArrayList<Pointer> p = new ArrayList<Pointer>();
		BTreeNode n = this.root;

		while (!(n instanceof BTreeLeafNode)) {
			BTreeInnerNode iN = (BTreeInnerNode) n;
			n = (BTreeNode) iN.children[0];
		}
		BTreeLeafNode lN = (BTreeLeafNode) n;

		while (lN != null) {
			overFlowPointers[] values = lN.values;
			int i = 0;
			while (values[i] != null) {
				overFlowPointers o = values[i];
				o.values.sort(null);
				for (int j = 0; j < o.values.size(); j++) {

					Pointer p1 = (Pointer) o.values.get(j);
					p.add(p1);
				}
				i++;
			}

			lN = (BTreeLeafNode) lN.rightSibling;
		}

		return p;
	}

	private Vector<BTreeLeafNode<TKey, TValue>> findchildern(BTreeNode<TKey> n) {

		Vector<BTreeLeafNode<TKey, TValue>> r = new Vector();
		int i = 0;
		BTreeNode<TKey> node1 = n;
		BTreeNode<TKey> child = n;
		int nodechildren = 0;

		if (n instanceof BTreeLeafNode) {

			r.add((BTreeLeafNode<TKey, TValue>) n);
		}

		if (n instanceof BTreeInnerNode) {
			nodechildren = ((BTreeInnerNode<TKey>) n).childrensize();

			child = ((BTreeInnerNode<TKey>) node1).getChild(0);

			if (child.getNodeType() != TreeNodeType.LeafNode) {

				for (int y = 0; y < nodechildren; y++) {

					child = ((BTreeInnerNode<TKey>) node1).getChild(y);
					r.addAll(findchildern(child));

				}

			} else {

				for (int y = 0; y < nodechildren; y++) {

					

					child = ((BTreeInnerNode<TKey>) node1).getChild(y);
					r.add((BTreeLeafNode<TKey, TValue>) child);

				}

			}

		}

		return r;

	}

	public Vector<overFlowPointers> searchselect(TKey key, String operator) {
		// BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);

		Vector<BTreeLeafNode<TKey, TValue>> r = new Vector();

		System.out.println("dd" + this.root.getClass());

		r = this.findchildern(this.root);

		// System.out.println("SIZE: " );
		// r.get(0).print();

		Vector<overFlowPointers> result = new Vector();

		Vector<Object> indices = new Vector();

		for (int j = 0; j < r.size(); j++) {

			BTreeLeafNode<TKey, TValue> leaf = r.get(j);

			indices = leaf.searchselectR(key, operator);

			for (int i = 0; i < indices.size(); i++) {

//				System.out.println("kfyaa");

//				System.out.println("Value:" + leaf.getValue((int) indices.get(i)).getValues());

				// for(int q=0; q<leaf.getValue((int) indices.get(i)).values.size();q++) {

				result.add((leaf.getValue((int) indices.get(i))));

				// System.out.println("HETEEEEEEEE" + (overFlowPointers) leaf.getValue((int)
				// indices.get(i)).values.get(q));

				// result.add(leaf.getValue((int) indices.get(i)));
				// }

			}
		}

		//System.out.println("yeaaahhhhhhh");

		for (int h = 0; h < result.size(); h++) {

//			System.out.println("HEREEEE");

			System.out.println(result.get(h).toString());
		}

		return result;

	}

	public static void main(String[] args) {

//		Polygon p = new Polygon();
//
//		p.addPoint(10, 10);
//		p.addPoint(20, 10);
//		p.addPoint(20, 20);
//		p.addPoint(10, 20);
//
//		Poly ps = new Poly();
//		ps.p = p;

//		Polygon p1 = new Polygon();
//		Poly ps1 = new Poly();
//
//		p1.addPoint(0, 0);
//		p1.addPoint(5, 0);
//		p1.addPoint(5, 5);
//		p1.addPoint(0, 5);
//		ps1.p = p1;
//
//		Polygon p2 = new Polygon();
//		Poly ps2 = new Poly();
//
//		p2.addPoint(0, 0);
//		p2.addPoint(10, 0);
//		p2.addPoint(10, 5);
//		p2.addPoint(0, 5);
//
//		ps2.p = p2;
//
//		Polygon p3 = new Polygon();
//		Poly ps3 = new Poly();
//
//		p3.addPoint(10, 0);
//		p3.addPoint(30, 0);
//		p3.addPoint(30, 10);
//		p3.addPoint(10, 10);
//
//		ps3.p = p3;
//
//		Polygon p4 = new Polygon();
//		Poly ps4 = new Poly();
//		p4.addPoint(0, 0);
//		p4.addPoint(10, 0);
//		p4.addPoint(10, 10);
//		p4.addPoint(0, 10);
//
//		ps4.p = p4;
//
//		Polygon p5 = new Polygon();
//		Poly ps5 = new Poly();
//
//		p5.addPoint(10, 10);
//		p5.addPoint(20, 10);
//		p5.addPoint(20, 20);
//		p5.addPoint(10, 20);
//
//		ps5.p = p5;
//
//		RTree BT1 = new RTree();
//		BT1.insert(ps5, new Pointer("aa", 5, 4));
//		BT1.insert(ps1, new Pointer("aa", 2, 1));
//		BT1.insert(ps2, new Pointer("aa", 3, 2));
//		BT1.insert(ps3, new Pointer("aa", 4, 3));
//		BT1.insert(ps4, new Pointer("aa", 4, 4));
//		BT1.insert(ps4, new Pointer("aa", 2, 2));
//
//		BT1.rprintTree();
//		System.out.println();
//
//		Vector<overFlowPointers> res = new Vector();
//
//		System.out.println("answer" + BT1.searchselect(ps5, "="));
//
//		System.out
//				.print(((Poly) ((((BTreeInnerNode) BT1.root).getChild(0)).rightSibling.getKey(1))).p.xpoints[0] + ",");
//		System.out.println(((Poly) ((((BTreeInnerNode) BT1.root).getChild(0)).rightSibling.getKey(1))).p.ypoints[0]);
//		System.out
//				.print(((Poly) ((((BTreeInnerNode) BT1.root).getChild(0)).rightSibling.getKey(1))).p.xpoints[1] + ",");
//
//		System.out.println(((Poly) ((((BTreeInnerNode) BT1.root).getChild(0)).rightSibling.getKey(1))).p.ypoints[1]);

//		BT1.insert("laaa", new Pointer("aa", 6, 0));
//		BT1.insert("Laaa", new Pointer("aa", 7, 1));
//		BT1.insert("what", new Pointer("aa", 8, 2));
//		BT1.insert("what", new Pointer("aa", 8, 3));
	}

}
