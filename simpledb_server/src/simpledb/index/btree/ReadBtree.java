package simpledb.index.btree;

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
	 HashMap<RID,Constant> values = new HashMap<>();

	public  HashMap<RID, Constant> getValues() {
		return values;
	}

	

	 void test(String fldname) {
		// TODO Auto-generated method stub
		SimpleDB.init("btreedeneme5");
		tx1 = new Transaction();
		md = SimpleDB.mdMgr();
		ti = md.getTableInfo("btreeinddleaf", tx1);
		// // System.out.println("filename:" +ti.fileName());
		leafblocknum = tx1.size(ti.fileName());
		recordnum = 0;
		blocknum = 0;
		skeleton();
		// Transaction tx1 = new Transaction();
		// MetadataMgr md = SimpleDB.mdMgr();
		// TableInfo ti = md.getTableInfo("btreeind7leaf", tx1);
		// System.out.println("filename:" +ti.fileName());
	}

	 void skeleton() {
		// int leafblocknum = tx1.size(ti.fileName());
		System.out.println("toplam blok sayýsý:" + leafblocknum);
		Block currentBlk = new Block(ti.fileName(), 0);
		BTreePage currentPage = new BTreePage(currentBlk, ti, tx1);
		// BTreeLeaf btleaf = new BTreeLeaf(currentBlk, ti, searchkey, tx1);
		int slot = 0;
		// int k = 0;
		// int l = 0;
		int z = 0;
		int newblknum = 0;
		BTreePage miniPage = null;
		while (currentPage.getNextLeaf() > 0 && currentPage.getNextLeaf() < leafblocknum) {
			System.out.println("next leaf var." + currentPage.getNextLeaf());
			newblknum = currentPage.getNextLeaf();
			slot = 0;
			System.out.println("" + blocknum + ". blok" + " gerçek blok no:" + currentPage.getCurrentblk().number());
			blocknum++;

			while (slot < currentPage.getNumRecs()) {
				System.out.println("" + recordnum + ". eleman " + currentPage.getDataVal(slot));
			
				values.put(currentPage.getDataRid(slot), currentPage.getDataVal(slot));
				if (currentPage.getFlag() > 0 && (z == 0)) {
					System.out.println("flag 0 dan büyük");
					z++;
					// int minislot = 0;
					// Block miniblk = new
					// Block(ti.fileName(),currentPage.getFlag());
					// if(miniPage != null)
					// miniPage.close();
					// miniPage = new BTreePage(miniblk, ti, tx1);
					// blocknum++;
					// while(minislot < miniPage.getNumRecs()){
					//// if(miniPage.getFlag() > 0 && (z==0)){
					//// System.out.println("overflow block bile overflow bloka
					// sabit");
					//// }
					//
					// recordnum++;
					// System.out.println(" "+recordnum+". eleman " +
					// miniPage.getDataVal(minislot));
					// minislot++;
					// }
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
		System.out.println("next leaf durumu:" + currentPage.getNextLeaf() + " blkNo:"
				+ currentPage.getCurrentblk().number() + " kayýt sayýsý:" + currentPage.getNumRecs());
		// if(currentPage.getCurrentblk().number() == (leafblocknum-1)){
		slot = 0;
		blocknum++;
		System.out.println("" + blocknum + ". blok");
		while (slot < currentPage.getNumRecs()) {
			
			System.out.println("" + recordnum + ". eleman " + currentPage.getDataVal(slot));
			values.put(currentPage.getDataRid(slot), currentPage.getDataVal(slot));
			if (currentPage.getFlag() > 0 && (z == 0)) {
				System.out.println("flag 0 dan büyük");
				z++;
				// findRecordsInOverflowBlk(currentPage, miniPage);
				// int minislot = 0;
				// Block miniblk = new
				// Block(ti.fileName(),currentPage.getFlag());
				// if(miniPage != null)
				// miniPage.close();
				// miniPage = new BTreePage(miniblk, ti, tx1);
				// blocknum++;
				// System.out.println(" kayýt sayýsý:"+ miniPage.getNumRecs());
				// while(minislot < miniPage.getNumRecs()){
				//
				// recordnum++;
				// System.out.println(" "+recordnum+". eleman " +
				// miniPage.getDataVal(minislot));
				// minislot++;
				// }
				findRecordsInOverflowBlk(currentPage, miniPage);
			}
			recordnum++;
			slot++;

		}
		System.out.println("toplam blok sayýsý:" + leafblocknum);
		 currentPage.close();
		// miniPage.close();
		// }
		// for(int i=0; i<leafblocknum; i++){
		// System.out.println("" + i +". blok");
		// currentBlk = new Block(ti.fileName(), i);
		// if(currentPage != null)
		// currentPage.close();
		// currentPage = new BTreePage(currentBlk, ti, tx1);
		// slot = 0;
		// while(slot < currentPage.getNumRecs()){
		// System.out.println(""+k+". eleman " + currentPage.getDataVal(slot));
		// k++;
		// slot++;
		// }
		// }
		//
		// BTreeIndex bidx = (BTreeIndex) idx;
		// ti = md.getTableInfo("btreeIdxleaf", tx);
		// RecordFile rf2 = new RecordFile(ti, tx);
		// if(ti.schema().hasField("block"))
		// System.out.println("yas alanýna sahip");
		// else
		// System.out.println("alana sahip deðil");

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
		System.out.println(" kayýt sayýsý:" + miniPage.getNumRecs());
		while (minislot < miniPage.getNumRecs()) {
			if (miniPage.getFlag() > 0 && (z == 0)) {
				System.out.println("overflowun da overflowu var");
				z++;
				miniminiblk = new Block(ti.fileName(), miniPage.getFlag());
				if (miniminiPage != null)
					miniminiPage.close();
				miniminiPage = new BTreePage(miniminiblk, ti, tx1);
				//blocknum++;
				
				findRecordsInOverflowBlk(miniPage, miniminiPage);

			}
			recordnum++;
			System.out.println(" " + recordnum + ". eleman " + miniPage.getDataVal(minislot));
			values.put(miniPage.getDataRid(minislot), miniPage.getDataVal(minislot));
			minislot++;
		}
		if (miniPage != null)
			miniPage.close();
		z = 0;
	}
}
