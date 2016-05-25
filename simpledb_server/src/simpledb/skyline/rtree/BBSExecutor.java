package simpledb.skyline.rtree;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.List;

import simpledb.skyline.bnl.InitOneTable;
import simpledb.spatialindex.rtree.Node;
import simpledb.spatialindex.rtree.RTree;
import simpledb.spatialindex.rtree.Statistics;
import simpledb.spatialindex.rtree.RTree.Data;
import simpledb.spatialindex.spatialindex.IEntry;
import simpledb.spatialindex.spatialindex.INode;
import simpledb.spatialindex.spatialindex.IQueryStrategy;
import simpledb.spatialindex.spatialindex.IStatistics;

public class BBSExecutor {
	RTreeLoader rtreeLoader;
	RTree tree;
	Statistics m_stats2 = new Statistics();
	ArrayList<String> skylineFields;
	Object[][] allTuples;
	public Object[][] getAllTuples() {
		return allTuples;
	}
	public BBSExecutor(ArrayList<String> skyfields){
		skylineFields = skyfields;
		allTuples = new Object[InitOneTable.allTuples.length][skylineFields.size()];
	}
	private List<Data> skylineData;
	private ArrayList<Object> skylinePoints = new ArrayList<>();

	public ArrayList<Object> getSkylinePoints() {
		return skylinePoints;
	}

	public void executeAllTreeOps(String inputFileName, int capacity, int clickCount) {
		rtreeLoader = new RTreeLoader(inputFileName, "tmp_rtree", capacity, clickCount, skylineFields, allTuples);
		tree = rtreeLoader.getTree();
		Runtime runtime2 = Runtime.getRuntime();

		BBSSkyline bbs = new BBSSkyline(tree, skylineFields);
		skylineData = bbs.execute();
		System.out.println("toplam bellek:"+runtime2.totalMemory() / 1000000 +" MB");
		System.out.println("kullanýlan bellek:"+(runtime2.totalMemory() - runtime2.freeMemory()) / 1000000 +" MB");
		System.out.println("bellek kullaným oraný:" + (runtime2.totalMemory() - runtime2.freeMemory())/runtime2.totalMemory()*100);
		print(skylineFields);
		System.out.print("disk eriþim : " + (tree.m_stats.getReads() - rtreeLoader.getTreeCreateDiskAccess()));
		System.out.println("rtree aðacý için istatistikler: " + calculateStatistics().toString());

		System.out.print("eriþilen index node:" + bbs.v.m_indexIO);
		System.out.print("-");
		System.out.print("eriþilen leaf node:" + bbs.v.m_leafIO);
		System.out.println("");
		System.out.println("disk kullanum alaný:"+ extraStrorageInfo(clickCount)/1024 +"KB" );

		for(Data data : skylineData){
			for (int j = 0; j < skylineFields.size(); j++) {
				skylinePoints.add(data.getShape().getMBR().getLow(j));
			}
		}
	}

	void print(ArrayList<String> skylineFields) {
		System.out.println("" + skylineData.size() + " skyline bulundu.");
		int indx = 0;

		for (RTree.Data data : skylineData) {
			for (int i = 0; i < skylineFields.size(); i++) {
				if (indx < skylineFields.size()) {
					indx++;
					System.out.print(" " + data.getShape().getMBR().getLow(i));
				} else {
					System.out.println();
					indx = 0;
					indx++;
					System.out.print(" " + data.getShape().getMBR().getLow(i));

				}

			}
		}
	}

	public IStatistics calculateStatistics() {
		m_stats2.reset();
		MyQueryStrategy qs = new MyQueryStrategy();
		int[] next = new int[] { tree.m_rootID };
		Node n = tree.readNode(next[0]);
		m_stats2.m_nodes++;
		int level = n.getLevel();
		for (int i = 1; i <= level; i++)
			m_stats2.m_nodesInLevel.add(new Integer(0));
		m_stats2.m_nodesInLevel.add(level, new Integer(1));
		m_stats2.m_treeHeight = level;
		while (true) {
			boolean[] hasNext = new boolean[] { false, false, false };
			qs.getNextEntry(n, next, hasNext);
			if ((hasNext[0] == true)) {
				n = tree.readNode(next[0]);
				m_stats2.m_nodes++;
				level = n.getLevel();
				int i = ((Integer) m_stats2.m_nodesInLevel.get(level)).intValue();
				m_stats2.m_nodesInLevel.set(level, new Integer(i + 1));
			} else if ((hasNext[1] == true)) {
				n = tree.readNode(next[0]);
				m_stats2.m_leafs++;

			} else if (hasNext[0] == false && hasNext[1] == false) // DONE
				break;
			else
				System.err.println("errorrr");

		}
		return (IStatistics) m_stats2.clone();

	}

	class MyQueryStrategy implements IQueryStrategy {
		private ArrayList ids = new ArrayList();
		private ArrayList leafids = new ArrayList();
		private boolean reachedBeforeLeaf = false;

		public void getNextEntry(IEntry entry, int[] nextEntry, boolean[] hasNext) {
			// Region r = entry.getShape().getMBR();

			// print node MBRs gnuplot style!

			// traverse only index nodes at levels 2 and higher.
			// if (entry instanceof INode && ((INode) entry).getLevel() > 1)
			if (!reachedBeforeLeaf && entry instanceof INode && ((INode) entry).getLevel() > 1) {
				for (int cChild = 0; cChild < ((INode) entry).getChildrenCount(); cChild++) {
					ids.add(new Integer(((INode) entry).getChildIdentifier(cChild)));
				}
			} else if (((INode) entry).getLevel() == 1) {
				reachedBeforeLeaf = true;
				for (int cChild = 0; cChild < ((INode) entry).getChildrenCount(); cChild++) {
					leafids.add(new Integer(((INode) entry).getChildIdentifier(cChild)));
				}
			}
			if (!ids.isEmpty()) {
				nextEntry[0] = ((Integer) ids.remove(0)).intValue();
				hasNext[0] = true;
				hasNext[1] = false;
			} else if (!leafids.isEmpty()) {
				nextEntry[0] = ((Integer) leafids.remove(0)).intValue();
				hasNext[0] = false;
				hasNext[1] = true;
			} else {
				hasNext[0] = false;
				hasNext[1] = false;

			}
		}
	};

	public long extraStrorageInfo(int clickCount){
		long length = 0;
		String dirName = "A:\\bitirme_workspace\\git\\simpledb_server";
		//System.out.println(dirName);
		File dir = new File(dirName);
		FilenameFilter filter = new FilenameFilter() {
			public boolean accept(File dir, String name) {

				return name.startsWith("tmp_rtree") ;

			}
		};
		File[] file = dir.listFiles(filter);
		
		//System.out.println("clc:"+clc);
		if (file == null) {
			System.out.println("Either dir does not exist or is not a directory");
		} else{
			for (int i = 0; i < file.length; i++) {
				String filename = file[i].getName();
				//System.out.println(filename);
				if(clickCount < 10){
					//System.out.println(""+filename.substring(9, 10));
					//System.out.println("");
					if(filename.substring(9, 10).equals(""+clickCount)){
						
						length += file[i].length();
					}
				}
				else{
					if(filename.substring(9, 11).equals(""+clickCount)){
						length += file[i].length();
					}
				}
				
			}
		}
		return length;
	}
}
