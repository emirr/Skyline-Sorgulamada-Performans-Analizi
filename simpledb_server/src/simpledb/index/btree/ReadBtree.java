package simpledb.index.btree;

import java.util.ArrayList;
import java.util.HashMap;

import simpledb.file.Block;
import simpledb.metadata.MetadataMgr;
import simpledb.query.Constant;
import simpledb.record.RID;
import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class ReadBtree {
	Transaction tx1;
	MetadataMgr md;
	TableInfo ti;
	// System.out.println("filename:" +ti.fileName());
	int leafblocknum;
	int recordnum = 0;
	int blocknum = 0;
	String fldname;
	ArrayList<RID> ids;
	//HashMap<String, ArrayList<RID>> values = new HashMap<>();

	

	public ArrayList<RID> getIds() {
		return ids;
	}

	void readInit(String fieldname, int clickCount) {
		// TODO Auto-generated method stub
		// SimpleDB.init("btreedeneme5");
		// SimpleDB.init(dbname);
		fldname = fieldname;
		tx1 = new Transaction();
		md = SimpleDB.mdMgr();
		//ti = md.getTableInfo("fl", tx1);
//		if(ti.schema() == null)
//			System.out.println("sallama filename þema:");
		/*******/
		ti = md.getTableInfo("bt"+ clickCount + fldname + "leaf", tx1);
		System.out.println("filename:" +ti.fileName());
		leafblocknum = tx1.size(ti.fileName());
		System.out.println("bt"+ fldname + "leaf blok no:" + leafblocknum);
		recordnum = 0;
		blocknum = 0;
		readAllLeafs();
		// Transaction tx1 = new Transaction();
		// MetadataMgr md = SimpleDB.mdMgr();
		// TableInfo ti = md.getTableInfo("btreeind7leaf", tx1);
		// System.out.println("filename:" +ti.fileName());
	}

	void readAllLeafs() {
		// int leafblocknum = tx1.size(ti.fileName());
		//System.out.println("toplam blok sayýsý:" + leafblocknum);
		Block currentBlk = new Block(ti.fileName(), 0);
		BTreePage currentPage = new BTreePage(currentBlk, ti, tx1);
//		if(!ids.isEmpty())
//			ids.clear();
		ids = new ArrayList<>();
		// BTreeLeaf btleaf = new BTreeLeaf(currentBlk, ti, searchkey, tx1);
		int slot = 0;
		// int k = 0;
		// int l = 0;
		int z = 0;
		int newblknum = 0;
		BTreePage miniPage = null;
		while (currentPage.getNextLeaf() > 0 && currentPage.getNextLeaf() < leafblocknum) {
			//System.out.println("next leaf var." + currentPage.getNextLeaf());
			newblknum = currentPage.getNextLeaf();
			slot = 0;
			//System.out.println("" + blocknum + ". blok" + " gerçek blok no:" + currentPage.getCurrentblk().number());
			blocknum++;

			while (slot < currentPage.getNumRecs()) {
				//System.out.println("" + recordnum + ". eleman " + currentPage.getDataVal(slot) + " rid:" + currentPage.getDataRid(slot));

				// values.put(currentPage.getDataRid(slot),
				// currentPage.getDataVal(slot));
				ids.add(currentPage.getDataRid(slot));
				if (currentPage.getFlag() > 0 && (z == 0)) {
					//System.out.println("flag 0 dan büyük");
					z++;

					findRecordsInOverflowBlk(currentPage, miniPage);
				}
				recordnum++;
				slot++;
			}
			currentBlk = new Block(ti.fileName(), newblknum);
			if (currentPage != null)
				currentPage.close();
			currentPage = new BTreePage(currentBlk, ti, tx1);
			z = 0;
		}
		//System.out.println("next leaf durumu:" + currentPage.getNextLeaf() + " blkNo:"
		//		+ currentPage.getCurrentblk().number() + " kayýt sayýsý:" + currentPage.getNumRecs());
		// if(currentPage.getCurrentblk().number() == (leafblocknum-1)){
		slot = 0;
		blocknum++;
		//System.out.println("" + blocknum + ". blok");
		while (slot < currentPage.getNumRecs()) {

			//System.out.println("" + recordnum + ". eleman " + currentPage.getDataVal(slot) + " rid:" + currentPage.getDataRid(slot));
			// values.put(currentPage.getDataRid(slot),
			// currentPage.getDataVal(slot));
			ids.add(currentPage.getDataRid(slot));
			if (currentPage.getFlag() > 0 && (z == 0)) {
				//System.out.println("flag 0 dan büyük");
				z++;

				findRecordsInOverflowBlk(currentPage, miniPage);
			}
			recordnum++;
			slot++;

		}
		//System.out.println("toplam blok sayýsý:" + leafblocknum);
		//values.put(fldname, ids);
		currentPage.close();
		tx1.commit();
		System.out.println("btree recordnum:" + recordnum);

	}

	void findRecordsInOverflowBlk(BTreePage currPage, BTreePage miniPage) {
		int minislot = 0;
		int z = 0;
		BTreePage miniminiPage = null;
		Block miniminiblk = null;
		Block miniblk = new Block(ti.fileName(), currPage.getFlag());
		if (miniPage != null)
			miniPage.close();
		miniPage = new BTreePage(miniblk, ti, tx1);
		blocknum++;
		//System.out.println(" kayýt sayýsý:" + miniPage.getNumRecs());
		while (minislot < miniPage.getNumRecs()) {
			if (miniPage.getFlag() > 0 && (z == 0)) {
				//System.out.println("overflowun da overflowu var");
				z++;
				miniminiblk = new Block(ti.fileName(), miniPage.getFlag());
				if (miniminiPage != null)
					miniminiPage.close();
				miniminiPage = new BTreePage(miniminiblk, ti, tx1);
				// blocknum++;

				findRecordsInOverflowBlk(miniPage, miniminiPage);

			}
			recordnum++;
			//System.out.println(" " + recordnum + ". eleman " + miniPage.getDataVal(minislot) + " rid:" + miniPage.getDataRid(minislot));
			//values.put(miniPage.getDataRid(minislot), miniPage.getDataVal(minislot));
			ids.add(miniPage.getDataRid(minislot));
			minislot++;
		}
		if (miniPage != null)
			miniPage.close();
		z = 0;
	}
}
