package simpledb.skyline.rtree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import simpledb.spatialindex.rtree.Node;
import simpledb.spatialindex.rtree.RTree;
import simpledb.spatialindex.rtree.RTree.Data;
import simpledb.spatialindex.rtree.RTree.NNEntry;
import simpledb.spatialindex.spatialindex.IData;
import simpledb.spatialindex.spatialindex.IEntry;
import simpledb.spatialindex.spatialindex.INearestNeighborComparator;
import simpledb.spatialindex.spatialindex.INode;
import simpledb.spatialindex.spatialindex.IShape;
import simpledb.spatialindex.spatialindex.ISpatialIndex;
import simpledb.spatialindex.spatialindex.IVisitor;
import simpledb.spatialindex.spatialindex.Point;
import simpledb.spatialindex.spatialindex.RWLock;

public class BBSSkyline {
	public static long NODE_ACCESSES = 0;
	RWLock m_rwLock = new RWLock();

	RTree tree;
	MyVisitor2 v;
	NNComparator2 nnc;
	private int upsize;
	ArrayList<String> skylineFields;

	public BBSSkyline(ISpatialIndex rtree, ArrayList<String> skylineFields) {
		tree = (RTree) rtree;
		v = new MyVisitor2();
		nnc = new NNComparator2();
		this.skylineFields = skylineFields;
	}

	public List<Data> execute() {
		m_rwLock.read_lock();
		try
		{
		List<Data> skylineEntries = new ArrayList<>();
		ArrayList queue = new ArrayList();
		double[] f1 = new double[skylineFields.size()];
		for (int i=0; i<skylineFields.size();i++){
			f1[i] = 0.0;
		}
		Point originPoint = new Point(f1);
		
		Node n = tree.readNode(tree.m_rootID);
		queue.add(tree.new NNEntry(n, 0.0));
		while (queue.size() != 0) {
			NNEntry first = (NNEntry) queue.remove(0);
			if (!isDominatedInSet(first, skylineEntries)) {
				if (first.m_pEntry instanceof Node) {
					n = (Node) first.m_pEntry;
					v.visitNode((INode) n);
					for (int cChild = 0; cChild < n.getM_children(); cChild++) {//dönen childler doðru mu
						IEntry e;
						if (n.getM_level() == 0)// node leaf ise
						{
							e = tree.new Data(n.getM_pData()[cChild], n.getM_pMBR()[cChild],
									n.getM_pIdentifier()[cChild]);
						} else {
							e = (IEntry) tree.readNode(n.getM_pIdentifier()[cChild]);
							
							}
						NNEntry e2 = tree.new NNEntry(e,nnc.getMinimumDistance(originPoint,e));
						if(!isDominatedInSet(e2, skylineEntries)){
							int loc = Collections.binarySearch(queue, e2, tree.new NNEntryComparator());
							if (loc >= 0)
							   queue.add(loc, e2);
							else
							   queue.add((-loc - 1), e2);
						}
						
/*bu comparator kýsmý mindist'e göre eleman yerleþtiriyor olmalý iyi kavra
						 NNEntry e2 = new NNEntry(e,
						 nnc.getMinimumDistance(query,
						 e));

						// Why don't I use a TreeSet here? See comment above...
						// int loc = Collections.binarySearch(queue, e2, new
						// NNEntryComparator());
						// if (loc >= 0)
						// queue.add(loc, e2);
						// else
						// queue.add((-loc - 1), e2);
						 
						 */
					}
				} else {
					// zaten skyline
					//if(first.m_pEntry.getShape() instanceof Point){
						//System.out.println("nokta ekleniyor. " + first.m_pEntry.getClass());
						skylineEntries.add((RTree.Data) first.m_pEntry);
					//}
					//else{
					//	System.out.println("bu bir nokta deðil.");
					//}
					
				}
			}
		}
		return skylineEntries;
		}finally
		{
			m_rwLock.read_unlock();
		}
		
	}
	class NNComparator2 implements INearestNeighborComparator
	{
		public double getMinimumDistance(IShape query, IEntry e)
		{
			IShape s = e.getShape();
			return query.getMinimumDistance(s);
		}
	}
	class MyVisitor2 implements IVisitor {
		public int m_indexIO = 0;
		public int m_leafIO = 0;

		public void visitNode(final INode n) {
			if (n.isLeaf())
				m_leafIO++;
			else
				m_indexIO++;
		}

		public void visitData(final IData d) {
			System.out.println("visitdata: "+d.getIdentifier());
			// the ID of this data entry is an answer to the query. I will just
			// print it to stdout.
		}
	}

	private boolean isDominatedInSet(RTree.NNEntry a, List<RTree.Data> entries) {
		for (RTree.Data entry : entries)
			if (compareDomination(a, entry) == -1) {
				return true;
			}
		return false;
	}

	public int compareDomination(RTree.NNEntry a, RTree.Data data) {

		int k = 0;
		// int i = 0;
		upsize = skylineFields.size();
		for (int i = 0; i < skylineFields.size(); i++) {

			//if (a.m_pEntry instanceof Node)
				k += deepCompare(a.m_pEntry.getShape().getMBR().getLow(i), data.getShape().getMBR().getLow(i));

			// k += deepCompare(input.getDouble(tempField),
			// window.getDouble(tempField));
		}
		if ((upsize != 0) && k == upsize)
			return 1;
		else {
			if ((upsize != 0) && k == -upsize)
				return -1;
			else
				return 0;
		}

	}

	public int deepCompare(double x, double y) {
		int m = -2;
		
		
			if ((double) x < (double) y)
				m = 1;
			else {
				if ((double) x > (double) y)
					m = -1;
				else {
					upsize--;
					m = 0;
				}

			}
		
		return m;
	}
}
