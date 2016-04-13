package simpledb.index.btree;

import java.util.Random;

import simpledb.index.Index;
import simpledb.metadata.MetadataMgr;
import simpledb.query.Plan;
import simpledb.query.TablePlan;
import simpledb.query.UpdateScan;
import simpledb.record.RecordFile;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class CreateBtree {
	public static void main(String[] args) {
		// List<Integer> randomList = new ArrayList<Integer>();
		// Schema idxsch = new Schema();
		// Transaction tx = new Transaction();
		// idxsch.addIntField("majorid");
		// Index idx = new BTreeIndex("btreeIdx", idxsch, tx);
		//
		// for (int i = 0; i < 20; i++) {
		// Random _rgen = new Random();
		// int fieldVal = _rgen.nextInt(20);
		// System.out.println("" + i + ".deðer: " + fieldVal);
		// idx.insert(fieldVal, i);
		// randomList.add(fieldVal);
		// }
		
		Schema idxsch = new Schema();
		MetadataMgr md = null;
		TableInfo ti ;
		int numberOfTuples = 1000;
		SimpleDB.init("btreedeneme6");
		Transaction tx = new Transaction();
		//if (SimpleDB.fileMgr().isNew()) {
			System.out.println("loading data");
			md = SimpleDB.mdMgr();
			tx = new Transaction();

			Schema sch = new Schema();

			//sch.addDoubleField("yas");
			sch.addIntField("yas");
			//sch.addStringField("bosyer", 380);

			md.createTable("inputBtree", sch, tx);
			ti = md.getTableInfo("inputBtree", tx);

			RecordFile rf = new RecordFile(ti, tx);
			while (rf.next())
				rf.delete();
			rf.beforeFirst();
			for (int id = 0; id < numberOfTuples; id++) {
				rf.insert();
				Random _rgen = new Random();
				int fieldVal = _rgen.nextInt(200);
				//int fieldVal = _rgen.nextInt(3);
				//double fieldVal = _rgen.nextDouble()*200;
				System.out.println("" + id + ".deðer: " + fieldVal);
				rf.setInt("yas", fieldVal);
				//rf.setDouble("yas", fieldVal);

			}
		//}
		
		//Transaction tx1 = new Transaction();
//		Plan p = new TablePlan("inputBtree2", tx);
//		UpdateScan rf1 = (UpdateScan) p.open();
		//RecordFile rf1 = new RecordFile(ti, tx);
		//System.out.println("tablo adý1:");
//		idxsch.addDoubleField("yas");
//		idxsch.addDoubleField("dataval");
		//idxsch.addIntField("yas");
		idxsch.addIntField("dataval");
		//idxsch.addDoubleField("dataval");
	    idxsch.addIntField("block");
	    idxsch.addIntField("id");
//	    if(tx != null)
//	    	System.out.println("mal index");
//	    else
//	    	System.out.println("alert alert");
		Index idx = new BTreeIndex("btree", idxsch, tx);
		Plan p = new TablePlan("inputBtree", tx);
		UpdateScan rf1 = (UpdateScan) p.open();
		//System.out.println("tablo adý:");
		int i = 0;
		while (rf1.next()){
			//System.out.println("rid:" +  rf1.getRid());
			System.out.println("before insert");
			idx.insert(rf1.getVal("yas"), rf1.getRid());
			System.out.println("after insert");
//			idx.beforeFirst(rf1.getVal("yas"));
//			idx.next();
			i++;
			System.out.println(""+ i+".deðer "+"btreeye eklenen deðer: "+ rf1.getVal("yas") );
//			if(idx.getDataRid() != null)
//				System.out.println("" + idx.getDataRid());
//			else
//				System.out.println("nulllll");
			
		}
		
		rf1.close();
	    idx.close();
	    tx.commit();
	    
//	    Transaction tx1 = new Transaction();
//		ti = md.getTableInfo("btreeIdxleaf", tx1);
//	   System.out.println("filename:" +ti.fileName());
//		RecordFile rf2 = new RecordFile(ti, tx1);

//		//rf2.beforeFirst();
	   // Plan p2 = new TablePlan("btreeIdxleaf", tx1);
		//UpdateScan rf2 = (UpdateScan) p2.open();
		//BTreeIndex bidx = (BTreeIndex) idx;
//		ti = md.getTableInfo("btreeIdxleaf", tx);
//		RecordFile rf2 = new RecordFile(ti, tx);
////		if(ti.schema().hasField("block"))
////			System.out.println("yas alanýna sahip");
////		else
////			System.out.println("alana sahip deðil");
//		while (rf2.next()){
//			System.out.println("hjhjhj");
//			System.out.println("deðer:" +rf2.getInt("data") +" rid:" );
//		}
//		rf1.close();
//		idx.close();
	}
}
