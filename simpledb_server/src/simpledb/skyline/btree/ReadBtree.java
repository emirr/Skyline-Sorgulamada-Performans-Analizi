package simpledb.skyline.btree;

import java.util.ArrayList;
import java.util.HashMap;

import simpledb.file.Block;
import simpledb.file.FileMgr;
import simpledb.index.btree.BTreePage;
import simpledb.metadata.MetadataMgr;
import simpledb.query.Constant;
import simpledb.record.RID;
import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.skyline.bnl.InitOneTable;
import simpledb.tx.Transaction;

public class ReadBtree {
	Transaction tx1;
	MetadataMgr md;
	TableInfo ti;
	// System.out.println("filename:" +ti.fileName());
	int leafblocknum;
	int leafaccess;
	int overflowaccess;
	int recordnum = 0;
	int blocknum = 0;
	String fldname;
	ArrayList<RID> ids;
	HashMap<String,Integer> visitedLeafNodeCountForOneFieldTree = new HashMap<>();
	public HashMap<String, Integer> getVisitedLeafNodeCountForOneFieldTree() {
		return visitedLeafNodeCountForOneFieldTree;
	}

	HashMap<String,Integer> visitedOFNodeCountForOneFieldTree = new HashMap<>();
	public HashMap<String, Integer> getVisitedOFNodeCountForOneFieldTree() {
		return visitedOFNodeCountForOneFieldTree;
	}
	Block currentBlk2;
	BTreePage currentPage2;
	int slot2 = 0;
	int minislot2;
	int z2 = 0;
	int newblknum2 = 0;
	BTreePage miniPage2;
	BTreePage miniminiPage2;
	// HashMap<String, ArrayList<RID>> values = new HashMap<>();
	ArrayList<RID> overFlowList;
	//ArrayList<Constant> overflowData;
	ArrayList<RID> yedekList =  new ArrayList<>();
	//ArrayList<Constant> yedekData = new ArrayList<>();
	//Constant ridVal ;

//	public Constant getRidVal() {
//		return ridVal;
//	}

	public ArrayList<RID> getIds() {
		return ids;
	}

//	void readInit(String fieldname, int clickCount) {
//		// TODO Auto-generated method stub
//		// SimpleDB.init("btreedeneme5");
//		// SimpleDB.init(dbname);
//		fldname = fieldname;
//		tx1 = new Transaction();
//		md = SimpleDB.mdMgr();
//		// ti = md.getTableInfo("fl", tx1);
//		// if(ti.schema() == null)
//		// System.out.println("sallama filename þema:");
//		/*******/
////		ti = md.getTableInfo("bt" + clickCount + fldname + "leaf", tx1);
//		ti = md.getTableInfo("bt"+ fldname + "leaf", tx1);
//		System.out.println("filename:" + ti.fileName());
//		leafblocknum = tx1.size(ti.fileName());
//		System.out.println("bt" + fldname + "leaf blok no:" + leafblocknum);
//		recordnum = 0;
//		blocknum = 0;
//		readAllLeafs();
//		// Transaction tx1 = new Transaction();
//		// MetadataMgr md = SimpleDB.mdMgr();
//		// TableInfo ti = md.getTableInfo("btreeind7leaf", tx1);
//		// System.out.println("filename:" +ti.fileName());
//	}

	RID readSync(String fieldname, int clickCount, int recordNo) {
		leafaccess = 0;
		RID rid = null;
		
		int diskerisim1 = (FileMgr.getReadCount() + FileMgr.getWriteCount());
		if (recordNo == 0) {
			fldname = fieldname;
			tx1 = new Transaction();
			md = SimpleDB.mdMgr();
			// ti = md.getTableInfo("fl", tx1);
			// if(ti.schema() == null)
			// System.out.println("sallama filename þema:");
			/*******/
			//ti = md.getTableInfo("bt" + clickCount + fldname + "leaf", tx1);
			ti = md.getTableInfo("bt"+ fldname + "leaf", tx1);
			
			//System.out.println("filename:" + ti.fileName());
			
			leafblocknum = tx1.size(ti.fileName());
			System.out.println("bt" + fldname + "leaf blok no:" + leafblocknum);
			recordnum = 0;
			blocknum = 0;
			currentBlk2 = new Block(ti.fileName(), 0);
			currentPage2 = new BTreePage(currentBlk2, ti, tx1);
			leafaccess++;
			int leafAccess;
			if(visitedLeafNodeCountForOneFieldTree.get(fieldname) == null)
				leafAccess = 0;
			else{
				leafAccess = visitedLeafNodeCountForOneFieldTree.get(fieldname);
				
			}
			leafAccess++;
			visitedLeafNodeCountForOneFieldTree.put(fieldname, leafAccess);
			//System.out.println("" + blocknum + ". blok" + " gerçek blok no:"
						// + currentPage2.getCurrentblk().number() + "record sayýsý:" + currentPage2.getNumRecs()+" flagdegeri:"+currentPage2.getFlag());
			
			miniPage2 = null;
			overFlowList = new ArrayList<>();
			//overflowData = new ArrayList<>();
		}

		// int k = 0;
		// int l = 0;

		

		if (slot2 % currentPage2.getNumRecs() == 0 && slot2 > 0) {
			newblknum2 = currentPage2.getNextLeaf();
			slot2 = 0;
			
			blocknum++;
			currentBlk2 = new Block(ti.fileName(), newblknum2);
			if (currentPage2 != null)
				currentPage2.close();
			currentPage2 = new BTreePage(currentBlk2, ti, tx1);
			//leafaccess++;
			int leafAccess;
			if(visitedLeafNodeCountForOneFieldTree.get(fieldname) == null)
				leafAccess = 0;
			else{
				leafAccess = visitedLeafNodeCountForOneFieldTree.get(fieldname);
			}
			leafAccess++;

			visitedLeafNodeCountForOneFieldTree.put(fieldname, leafAccess);
			//System.out.println("" + blocknum + ". blok" + " gerçek blok no:"
						// + currentPage2.getCurrentblk().number());
			
			overFlowList = new ArrayList<>();
			//overflowData = new ArrayList<>();
			z2 = 0;
		}
		// ids.add(currentPage.getDataRid(slot));
//		if (!(currentPage2.getFlag() > 0)) {
//			// ids.add(currentPage2.getDataRid(slot2)) ;
//			rid = currentPage2.getDataRid(slot2);
//		}
		// ids.add(currentPage.getDataRid(slot));

		if (currentPage2.getFlag() > 0 && z2 == 0) {
			findRecordsInOverflowBlk(currentPage2, miniPage2, fieldname);
			yedekList = overFlowList;
			yedekList.add(0, currentPage2.getDataRid(slot2));
			slot2++;
			z2++;
		}
		
		if(yedekList.isEmpty()){
			rid = currentPage2.getDataRid(slot2);
			//ridVal = currentPage2.getDataVal(slot2);
			slot2++;
		}
		else{
			rid = yedekList.get(0);
			//ridVal = overflowData.get(0);
			yedekList.remove(0);
			//overflowData.remove(0);

		}
		
		// if (currentPage.getFlag() > 0 && (z == 0)) {
		//
		// z++;
		//
		//
		//
		//

		recordnum++;
//		System.out.println("gelen recno1:"+recordnum);
//		System.out.println("el recno1:"+recordnum);
		if(recordnum >= InitOneTable.allTuples.length || recordNo >= InitOneTable.allTuples.length ){
			
			//System.out.println("gelen recno:"+recordNo);
			//System.out.println("eldeki recno:"+recordnum);
			
			currentPage2.close();
			tx1.commit();
		}

		return rid;

	}

//	void readAllLeafs() {
//		// int leafblocknum = tx1.size(ti.fileName());
//		// System.out.println("toplam blok sayýsý:" + leafblocknum);
//		int diskerisim3 = (FileMgr.getReadCount() + FileMgr.getWriteCount());
//
//		Block currentBlk = new Block(ti.fileName(), 0);
//		BTreePage currentPage = new BTreePage(currentBlk, ti, tx1);
//		// if(!ids.isEmpty())
//		// ids.clear();
//		ids = new ArrayList<>();
//		// BTreeLeaf btleaf = new BTreeLeaf(currentBlk, ti, searchkey, tx1);
//		int slot = 0;
//		// int k = 0;
//		// int l = 0;
//		int z = 0;
//		int newblknum = 0;
//		BTreePage miniPage = null;
//		int diskerisim4 = (FileMgr.getReadCount() + FileMgr.getWriteCount());
//		System.out.println("readinitdeki block ayarlama eriþim sayýsý:" + (diskerisim4 - diskerisim3));
//
//		int diskerisim1 = (FileMgr.getReadCount() + FileMgr.getWriteCount());
//		while (currentPage.getNextLeaf() > 0 && currentPage.getNextLeaf() < leafblocknum) {
//			// System.out.println("next leaf var." + currentPage.getNextLeaf());
//			newblknum = currentPage.getNextLeaf();
//			slot = 0;
//			// System.out.println("" + blocknum + ". blok" + " gerçek blok no:"
//			// + currentPage.getCurrentblk().number());
//			blocknum++;
//
//			while (slot < currentPage.getNumRecs()) {
//				// System.out.println("" + recordnum + ". eleman " +
//				// currentPage.getDataVal(slot) + " rid:" +
//				// currentPage.getDataRid(slot));
//
//				// values.put(currentPage.getDataRid(slot),
//				// currentPage.getDataVal(slot));
//				ids.add(currentPage.getDataRid(slot));
//				if (currentPage.getFlag() > 0 && (z == 0)) {
//					// System.out.println("flag 0 dan büyük");
//					z++;
//
//					findRecordsInOverflowBlk(currentPage, miniPage);
//				}
//				recordnum++;
//				slot++;
//			}
//			currentBlk = new Block(ti.fileName(), newblknum);
//			if (currentPage != null)
//				currentPage.close();
//			currentPage = new BTreePage(currentBlk, ti, tx1);
//			z = 0;
//		}
//		// System.out.println("next leaf durumu:" + currentPage.getNextLeaf() +
//		// " blkNo:"
//		// + currentPage.getCurrentblk().number() + " kayýt sayýsý:" +
//		// currentPage.getNumRecs());
//		// if(currentPage.getCurrentblk().number() == (leafblocknum-1)){
//		slot = 0;
//		blocknum++;
//		// System.out.println("" + blocknum + ". blok");
//		while (slot < currentPage.getNumRecs()) {
//
//			// System.out.println("" + recordnum + ". eleman " +
//			// currentPage.getDataVal(slot) + " rid:" +
//			// currentPage.getDataRid(slot));
//			// values.put(currentPage.getDataRid(slot),
//			// currentPage.getDataVal(slot));
//			ids.add(currentPage.getDataRid(slot));
//			if (currentPage.getFlag() > 0 && (z == 0)) {
//				// System.out.println("flag 0 dan büyük");
//				z++;
//
//				findRecordsInOverflowBlk(currentPage, miniPage);
//			}
//			recordnum++;
//			slot++;
//
//		}
//		int diskerisim2 = (FileMgr.getReadCount() + FileMgr.getWriteCount());
//		System.out.println("readinitdeki disk eriþim sayýsý:" + (diskerisim2 - diskerisim1));
//		// System.out.println("toplam blok sayýsý:" + leafblocknum);
//		// values.put(fldname, ids);
//		currentPage.close();
//		tx1.commit();
//		System.out.println("btree recordnum:" + recordnum);
//
//	}

	void findRecordsInOverflowBlk(BTreePage currPage, BTreePage miniPage, String fieldname) {
		overflowaccess = 0;
		int minislot = 0;
		int z = 0;
		BTreePage miniminiPage = null;
		Block miniminiblk = null;
		Block miniblk = new Block(ti.fileName(), currPage.getFlag());
		if (miniPage != null)
			miniPage.close();
		miniPage = new BTreePage(miniblk, ti, tx1);
		overflowaccess++;
		int overFlowAccess;
		if(visitedOFNodeCountForOneFieldTree.get(fldname) == null)
			overFlowAccess = 0;
		else
			overFlowAccess = visitedOFNodeCountForOneFieldTree.get(fldname);
		overFlowAccess++;
		visitedOFNodeCountForOneFieldTree.put(fieldname, overFlowAccess);

		blocknum++;
		// System.out.println(" kayýt sayýsý:" + miniPage.getNumRecs());
		while (minislot < miniPage.getNumRecs()) {
			if (miniPage.getFlag() > 0 && (z == 0)) {
				// System.out.println("overflowun da overflowu var");
				z++;
				miniminiblk = new Block(ti.fileName(), miniPage.getFlag());
				if (miniminiPage != null)
					miniminiPage.close();
				miniminiPage = new BTreePage(miniminiblk, ti, tx1);
				//overflowaccess++;
				overFlowAccess++;
				visitedOFNodeCountForOneFieldTree.put(fieldname, overFlowAccess);
				findRecordsInOverflowBlk(miniPage, miniminiPage, fieldname);

			}
			recordnum++;
			// System.out.println(" " + recordnum + ". eleman " +
			// miniPage.getDataVal(minislot) + " rid:" +
			// miniPage.getDataRid(minislot));
			// values.put(miniPage.getDataRid(minislot),
			// miniPage.getDataVal(minislot));
			// ids.add(miniPage.getDataRid(minislot));
			overFlowList.add(miniPage.getDataRid(minislot));
			//overflowData.add(miniPage.getDataVal(minislot));
			minislot++;
		}
		if (miniPage != null)
			miniPage.close();
		z = 0;
	}
}
