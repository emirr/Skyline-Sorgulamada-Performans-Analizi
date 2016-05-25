package simpledb.skyline.btree;

import static java.sql.Types.DOUBLE;
import static java.sql.Types.INTEGER;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.HashMap;

import simpledb.file.Block;
import simpledb.file.FileMgr;
import simpledb.index.btree.BTreePage;
import simpledb.index.btree.IStatistics;
import simpledb.index.btree.MyQueryStrategy1;
import simpledb.index.btree.Statistics;
import simpledb.metadata.MetadataMgr;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class ExecutorForBtree {
	SkylineForBtree s4bt;
	private Statistics m_stats = new Statistics();
	private TableInfo dirTi, leafTi;
	static HashMap<String,Integer> heightForEachTree = new HashMap<>();
	long  storageSize;
	String dbName;
	ArrayList<String> slctdFileds;
	public long getStorageSize() {
		return storageSize;
	}

	public SkylineForBtree getS4bt() {
		return s4bt;
	}

	public ExecutorForBtree(String dbName) {
		s4bt = new SkylineForBtree();
		this.dbName = dbName;
	}

	public void exec4Btree(int clickcount, int buffsize, String tblName, ArrayList<String> selectedSkylineFields) {
		slctdFileds = selectedSkylineFields;
		Transaction tx = new Transaction();
		MetadataMgr md = SimpleDB.mdMgr();
		for (String fieldName : selectedSkylineFields) {
			// seçilen özelliðin indexi yoksa oluþtur.
			if (SimpleDB.mdMgr().getIndexInfo(tblName, tx).get(fieldName) == null) {
				CreateBtree.loadIndex(tblName, fieldName, "bt" + fieldName, tx);
			}
			TableInfo ti,ti1,ti2;
			//Transaction tx = new Transaction();
			ti = SimpleDB.mdMgr().getTableInfo(tblName, tx);
			
			String leaftbl = "bt" + fieldName + "leaf";
			String dirtbl = "bt" + fieldName + "dir";
			dirTi = md.getTableInfo(dirtbl, tx); 
//			Schema leafSch = new Schema();
//			if (ti.schema().type(fieldName) == INTEGER)
//				leafSch.addIntField("dataval");
//			if (ti.schema().type(fieldName) == DOUBLE)
//				leafSch.addDoubleField("dataval");
//
//			leafSch.addIntField("block");
//			leafSch.addIntField("id");
			leafTi = md.getTableInfo(leaftbl, tx); 
			
//			Schema dirsch = new Schema();
//		      dirsch.add("block",   leafSch);
//		      dirsch.add("dataval", leafSch);
//			dirTi = new TableInfo(dirtbl, leafSch);
			
		System.out.println(" "+fieldName+" alaný için btree istatistikleri:"+ calculateStatistics(fieldName, tx).toString());
			
			
			
			
		}
		tx.commit();
		int diskErisimSayýsý = (FileMgr.getReadCount() + FileMgr.getWriteCount());
		s4bt.findSkylineNominates(tblName, selectedSkylineFields, clickcount);
		int diskErisimSayýsý2 = (FileMgr.getReadCount() + FileMgr.getWriteCount());
		//System.out.println("btreeler haricinde -ara- disk eriþim sayýsý:" + (diskErisimSayýsý2 - diskErisimSayýsý));
		s4bt.skylineFinder(clickcount, buffsize);
		extraStrorageInfo();
		System.out.println("btree yönteminin kullandýðý disk alaný:"+ storageSize/1024+"KB");
		//int diskErisimSayýsý3 = (FileMgr.getReadCount() + FileMgr.getWriteCount());

		// System.out.println("sonraki disk eriþim sayýsý:" +
		// diskErisimSayýsý2);
//		System.out.println("btreeler haricinde disk eriþim sayýsý:" + (diskErisimSayýsý3 - diskErisimSayýsý2));
//		System.out.println("en dýþ disk:" + (diskErisimSayýsý3 - diskErisimSayýsý));
		// s4bt.print();
//		for (String fldname : selectedSkylineFields) {
//			
//			
//		}

	}

	public IStatistics calculateStatistics(String fieldName, Transaction tx) { // breath-first
			
		m_stats.reset();

			
			MyQueryStrategy1 qs = new MyQueryStrategy1();
			int[] next = new int[] { 0 }; // root'dan basliyoruz.
			Block blk = new Block(dirTi.fileName(), next[0]);
			BTreePage page = new BTreePage(blk, dirTi, tx);
			m_stats.m_nodes++;
			int level = page.getFlag();
			
			for (int i = 0; i < level; i++)
				m_stats.m_nodesInLevel.add(new Integer(0));
			m_stats.m_nodesInLevel.add(level, new Integer(1));
			m_stats.m_treeHeight = level + 1;
			heightForEachTree.put(fieldName, (level+1));
			while (true) {
				boolean[] hasNext = new boolean[] { false, false, false };
				qs.getNextEntry(page, next, hasNext);

				if ((hasNext[0] == true)) { // DIR pages.
					blk = new Block(dirTi.fileName(), next[0]);
					page = new BTreePage(blk, dirTi, tx);
					m_stats.m_nodes++;
					level = page.getFlag();
					int i = ((Integer) m_stats.m_nodesInLevel.get(level)).intValue();
					m_stats.m_nodesInLevel.set(level, new Integer(i + 1));
				} else if ((hasNext[1] == true)) { // LEAF pages.
					blk = new Block(leafTi.fileName(), next[0]);
					//System.out.println("leaf");
					page = new BTreePage(blk, leafTi, tx);
					m_stats.m_leafs++;
					//System.out.println("leaf sayýsý:" + m_stats.m_leafs++);
					m_stats.m_data += page.getNumRecs();
				} else if ((hasNext[2] == true)) { // OF pages.
					blk = new Block(leafTi.fileName(), next[0]);
					page = new BTreePage(blk, leafTi, tx);
					m_stats.m_OFnodes++;
					m_stats.m_data += page.getNumRecs();
				} else if (hasNext[0] == false && hasNext[1] == false) // DONE
					break;
				else
					System.err.println("errorrr");
			}
			return (IStatistics) m_stats.clone();

	}
	public void extraStrorageInfo(){
		String dirName = "C:\\Users\\oblmv2" + "\\" + dbName;
		System.out.println(dirName);
		File dir = new File(dirName);
		FilenameFilter filter = new FilenameFilter() {
			public boolean accept(File dir, String name) {

				return name.startsWith("bt") ;

			}
		};
		File[] file = dir.listFiles(filter);
		if (file == null) {
			System.out.println("Either dir does not exist or is not a directory");
		} else {
			for (int i = 0; i < file.length; i++) {
				String filename = file[i].getName();
				System.out.println(filename);
				String fldname = filename.substring(2, 3);
				if(slctdFileds.contains(fldname))
					storageSize += file[i].length();
			}
		}
	}
}
