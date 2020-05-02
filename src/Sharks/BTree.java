package Sharks;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Vector;

/**
 * A B+ tree Since the structures and behaviors between internal node and
 * external node are different, so there are two different classes for each kind
 * of node.
 * 
 * @param <TKey> the data type of the key
 * @param <TValue> the data type of the value
 */
public class BTree<TKey extends Comparable<TKey>, TValue> implements Serializable {
	public BTreeNode<TKey> root;

	public BTree() {

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
			throw new DBAppException("This pointer" + (Pointer) value + "doesn't exist within this index" + key);

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
	public void updatePointerValue(TKey key, TValue value, TValue newValue) throws DBAppException {
		this.search(key).replace(value, newValue);
		;
	}

	public void saveBTree(String strTableName, String strColName) throws IOException {

		ObjectOutputStream output = new ObjectOutputStream(
				new FileOutputStream("data\\" + "BTree" + strTableName + "On" + strColName + ".class"));
		output.writeObject(this);// rewrites the pages back
		output.flush();
		output.close();

	}

	public void printTree() {
		this.root.print();
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

		r = this.findchildern(this.root);

		// System.out.println("SIZE: " );
		// r.get(0).print();

		Vector<overFlowPointers> result = new Vector();

		Vector<Object> indices = new Vector();

		for (int j = 0; j < r.size(); j++) {

			BTreeLeafNode<TKey, TValue> leaf = r.get(j);

			indices = leaf.searchselect(key, operator);

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

		// System.out.println("yeaaahhhhhhh");

		// for (int h = 0; h < result.size(); h++) {

		// System.out.println("HEREEEE");

		// System.out.println(result.get(h).toString());
		// }

		return result;

	}

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws DBAppException {
		
//		BTree BT = new BTree();
//		BT.insert(1, new Pointer("aa",1,1));
//		BT.insert(2, new Pointer("aa",1,2));
//		BT.insert(3, new Pointer("aa",1,3));
//		BT.insert(4, new Pointer("aa",1,4));
//		BT.insert(5, new Pointer("aa",1,5));
//		BT.insert(6, new Pointer("aa",1,6));
//		BT.insert(7, new Pointer("aa",1,7));
//		BT.insert(8, new Pointer("aa",1,8));
//		BT.insert(86, new Pointer("aa",1,9));
//		BT.insert(15, new Pointer("aa",1,10));
//		BT.insert(14, new Pointer("aa",1,11));
//		BT.insert(17, new Pointer("aa",1,12));
//		BT.insert(18, new Pointer("aa",1,13));
//		BT.insert(19, new Pointer("aa",1,14));
//		BT.insert(10, new Pointer("aa",1,15));
//		BT.insert(-10, new Pointer("aa",1,16));
//		BT.insert(-1, new Pointer("aa",1,17));

//		BTree BT1 = new BTree();
//		BT1.insert("BOO", new Pointer("aa", 1, 0));
//		BT1.insert("coo", new Pointer("aa", 2, 1));
//		BT1.insert("Faaaaaa", new Pointer("aa", 3, 2));
//		BT1.insert("YOO", new Pointer("aa", 4, 3));
//		BT1.insert("BOO", new Pointer("aa", 5, 4));
//		BT1.insert("laaa", new Pointer("aa", 6, 0));
//		BT1.insert("Laaa", new Pointer("aa", 7, 1));
//		BT1.insert("what", new Pointer("aa", 8, 2));
//		BT1.insert("what", new Pointer("aa", 8, 3));

		// System.out.println("OOOOOOOOOOOOOOO");

		// System.out.println(((BTreeLeafNode)(((BTreeInnerNode)
		// BT1.root).getChild(0))).getValue(2));

		//BT1.printTree();

//		BT1.searchselect("Laaa", "!=");
//		System.out.println();
//
//		ArrayList<Pointer> s = BT1.getLeafPointers();
//		s.sort(null);
//		System.out.println(s);

	}

}
