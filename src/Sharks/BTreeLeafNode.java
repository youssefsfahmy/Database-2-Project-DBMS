package Sharks;

import java.awt.Polygon;
import java.io.FileReader;
import java.util.Arrays;
import java.util.Properties;
import java.util.Vector;

class BTreeLeafNode<TKey extends Comparable<TKey>, TValue> extends BTreeNode<TKey> {
	protected static int LEAFORDER = -1;
	public overFlowPointers[] values;

	public BTreeLeafNode() {
		if (LEAFORDER == -1) {

			try {
				FileReader reader;
				reader = new FileReader("config\\DBAPP.properties");
				Properties p = new Properties();
				p.load(reader);
				LEAFORDER = Integer.parseInt(p.getProperty("NodeSize"));

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		this.keys = new Object[LEAFORDER + 1];
		this.values = new overFlowPointers[LEAFORDER + 1];
	}

	@SuppressWarnings("unchecked")
	public overFlowPointers getValue(int index) {
		return (overFlowPointers) this.values[index];
	}

	public void setValue(int index, TValue value) {
		if (this.values[index] == null) {
			this.values[index] = new overFlowPointers();
		}

		this.values[index].setValue(value);
	}

	public void repValue(int index, TValue value) {
		this.values[index] = new overFlowPointers();
		this.values[index].setValue(value);
	}

	@Override
	public TreeNodeType getNodeType() {
		return TreeNodeType.LeafNode;
	}

	@Override
	public int search(TKey key) {
		for (int i = 0; i < this.getKeyCount(); ++i) {
			int cmp = this.getKey(i).compareTo(key);
			if (cmp == 0) {
				return i;
			} else if (cmp > 0) {
				return -1;
			}
		}

		return -1;
	}

	/* The codes below are used to support insertion operation */

	public void insertKey(TKey key, TValue value) {
		int index = 0;
		while (index < this.getKeyCount() && this.getKey(index).compareTo(key) <= 0) {
			if (this.getKey(index).compareTo(key) == 0) {
				System.out.println("DUPPPk"); // THIS IS WHERE WE FIND THE DUPPLICATES

				this.setValue(index, value);

				return;
			}
			++index;
		}

		this.insertAt(index, key, value);
	}

	private void insertAt(int index, TKey key, TValue value) {
		// move space for the new key

		for (int i = this.getKeyCount() - 1; i >= index; --i) {
			this.setKey(i + 1, this.getKey(i));
			this.setValueShift(i + 1, this.getValue(i));

		}

		// insert new key and value
		this.setKey(index, key);
		this.repValue(index, value);
		++this.keyCount;

	}

	private void setValueShift(int i, overFlowPointers value) {
		this.values[i] = value;

	}

	/**
	 * When splits a leaf node, the middle key is kept on new node and be pushed to
	 * parent node.
	 */
	@Override
	protected BTreeNode<TKey> split() {
		int midIndex = this.getKeyCount() / 2;

		BTreeLeafNode<TKey, TValue> newRNode = new BTreeLeafNode<TKey, TValue>();
		for (int i = midIndex; i < this.getKeyCount(); ++i) {
			newRNode.setKey(i - midIndex, this.getKey(i));
			newRNode.setValueShift(i - midIndex, this.getValue(i));
			this.setKey(i, null);
			this.setValueShift(i, null);
		}
		newRNode.keyCount = this.getKeyCount() - midIndex;
		this.keyCount = midIndex;

		return newRNode;
	}

	@Override
	protected BTreeNode<TKey> pushUpKey(TKey key, BTreeNode<TKey> leftChild, BTreeNode<TKey> rightNode) {
		throw new UnsupportedOperationException();
	}

	/* The codes below are used to support deletion operation */

	public boolean delete(TKey key) {
		int index = this.search(key);
		if (index == -1)
			return false;

		this.deleteAt(index);
		return true;
	}

	private void deleteAt(int index) {
		int i = index;
		for (i = index; i < this.getKeyCount() - 1; ++i) {
			this.setKey(i, this.getKey(i + 1));
			this.setValueShift(i, this.getValue(i + 1));
		}
		this.setKey(i, null);
		this.setValueShift(i, null);
		--this.keyCount;
	}

	@Override
	protected void processChildrenTransfer(BTreeNode<TKey> borrower, BTreeNode<TKey> lender, int borrowIndex) {
		throw new UnsupportedOperationException();
	}

	@Override
	protected BTreeNode<TKey> processChildrenFusion(BTreeNode<TKey> leftChild, BTreeNode<TKey> rightChild) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Notice that the key sunk from parent is be abandoned.
	 */
	@Override
	@SuppressWarnings("unchecked")
	protected void fusionWithSibling(TKey sinkKey, BTreeNode<TKey> rightSibling) {
		BTreeLeafNode<TKey, TValue> siblingLeaf = (BTreeLeafNode<TKey, TValue>) rightSibling;

		int j = this.getKeyCount();
		for (int i = 0; i < siblingLeaf.getKeyCount(); ++i) {
			this.setKey(j + i, siblingLeaf.getKey(i));
			this.setValueShift(j + i, siblingLeaf.getValue(i));
		}
		this.keyCount += siblingLeaf.getKeyCount();

		this.setRightSibling(siblingLeaf.rightSibling);
		if (siblingLeaf.rightSibling != null)
			siblingLeaf.rightSibling.setLeftSibling(this);
	}

	@Override
	@SuppressWarnings("unchecked")
	protected TKey transferFromSibling(TKey sinkKey, BTreeNode<TKey> sibling, int borrowIndex) {
		BTreeLeafNode<TKey, TValue> siblingNode = (BTreeLeafNode<TKey, TValue>) sibling;

		this.insertKeyS(siblingNode.getKey(borrowIndex), siblingNode.getValue(borrowIndex));
		siblingNode.deleteAt(borrowIndex);

		return borrowIndex == 0 ? sibling.getKey(0) : this.getKey(0);
	}

	private void insertKeyS(TKey key, overFlowPointers value) { // Shift insert helper
		int index = 0;
		while (index < this.getKeyCount() && this.getKey(index).compareTo(key) <= 0) {
			if (this.getKey(index).compareTo(key) == 0) {
				System.out.println("DUPPP"); // THIS IS WHERE WE FIND THE DUPPLICATES
				return;

			}
			++index;
		}

		this.insertAtS(index, key, value);

	}

	private void insertAtS(int index, TKey key, overFlowPointers value) { // Shift insert
		for (int i = this.getKeyCount() - 1; i >= index; --i) {
			this.setKey(i + 1, this.getKey(i));
			this.setValueShift(i + 1, this.getValue(i));
		}

		// insert new key and value
		this.setKey(index, key);
		this.setValueShift(index, value);
		++this.keyCount;

	}

	// Martha Rtree
	public void rprint() {
		try {
			this.rprintK();
		} catch (DBAppException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public void print() {
		this.printK();

	}

	public Vector<Object> searchselect(TKey key, String operator) {

		Vector<Object> result = new Vector();

		for (int i = 0; i < this.getKeyCount(); i++) {

//			TKey comp=this.getKey(i);
//			
//			System.out.println("ANA WSLT HENE");
//			
//			if(comp instanceof String) {
//				
//				comp=(TKey) ((String) comp).toLowerCase();
//				key=(TKey) ((String) comp).toLowerCase();
//				
//				System.out.println("YEAHHHHHHHHHHHHHHHH");
//				
//			}
			
			

			if (operator.equals("=")) {

				// System.out.println(cmp);
				int cmp = this.getKey(i).compareTo(key);
				// System.out.println(cmp);

				if (cmp == 0) {

					System.out.println("equal");
					result.add(i);

				}

			}

			if (operator.equals("<")) {
				int cmp = this.getKey(i).compareTo(key);
				if (cmp < 0) {
					result.add(i);
				}

			}

			if (operator.equals("<=")) {
				int cmp = this.getKey(i).compareTo(key);
				if (cmp < 0 || cmp == 0) {
					result.add(i);
				}

			}

			if (operator.equals(">")) {
				int cmp = this.getKey(i).compareTo(key);
				if (cmp > 0) {
					result.add(i);
				}

			}

			if (operator.equals(">=")) {
				int cmp = this.getKey(i).compareTo(key);
				if (cmp > 0 || cmp == 0) {
					result.add(i);
				}

			}

			if (operator.equals("!=")) {
				int cmp = this.getKey(i).compareTo(key);

				if (cmp != 0) {
					result.add(i);
				}

			}

		}

		return result;
	}

	public Vector<Object> searchselectR(TKey key, String operator) {

		Vector<Object> result = new Vector();

		for (int i = 0; i < this.getKeyCount(); i++) {
			
		//	System.out.println("count" + this.getKey(i).getClass());
			
			
//			for(int j=0; j<((BTreeLeafNode)this.getKey(i)).values.length;j++) {
//				
//				System.out.println("ncjaws");
//			}

			if (operator.equals("=")) {

				// System.out.println(cmp);
				int cmp = ((Poly) this.getKey(i)).compareToSelectRTree((Poly) key, operator);
				// System.out.println(cmp);
				
				

				if (cmp == 0) {

					System.out.println("equal");
					result.add(i);

				}

			}

			if (operator.equals("<")) {
				int cmp = ((Poly) this.getKey(i)).compareToSelectRTree((Poly) key, operator);
				if (cmp < 0) {
					result.add(i);
				}

			}

			if (operator.equals("<=")) {
				int cmp = ((Poly) this.getKey(i)).compareToSelectRTree((Poly) key, operator);
				if (cmp < 0 || cmp == 0) {
					result.add(i);
				}

			}

			if (operator.equals(">")) {
				int cmp = ((Poly) this.getKey(i)).compareToSelectRTree((Poly) key, operator);
				if (cmp > 0) {
					result.add(i);
				}

			}

			if (operator.equals(">=")) {
				int cmp = ((Poly) this.getKey(i)).compareToSelectRTree((Poly) key, operator);
				if (cmp > 0 || cmp == 0) {
					result.add(i);
				}

			}

			if (operator.equals("!=")) {
			//	int cmp = ((Poly) this.getKey(i)).compareToSelectRTree((Poly) key, operator);
				
			//	System.out.println( i +" "+ "grjg" + ((Poly) this.getKey(i)).p.getBoundingBox());
				
			//	System.out.println("cmp" + cmp);

			//	if (cmp != 0) {
					result.add(i);
			//	}

			}

		}

		return result;
	}

}
