package simpledb.skyline.btree;

import static java.sql.Types.DOUBLE;
import static java.sql.Types.INTEGER;

import java.util.ArrayList;
import java.util.Map;

import simpledb.index.Index;
import simpledb.index.btree.BTreeIndex;
import simpledb.metadata.IndexInfo;
import simpledb.metadata.MetadataMgr;
import simpledb.query.Plan;
import simpledb.query.TablePlan;
import simpledb.query.UpdateScan;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class CreateBtree {
	public static void bTreeCreate(String tblName, ArrayList<String> selectedSkylineFields, int clickCount) {

		Schema idxsch = new Schema();
		MetadataMgr md = null;
		TableInfo ti, ti2;
		// int numberOfTuples = 1000;
		// SimpleDB.init("btreedeneme6");
		Transaction tx = new Transaction();
		// if (SimpleDB.fileMgr().isNew()) {
		System.out.println("her bir skyline alaný için btree oluþturma iþlemi baþlýyor.");
		md = SimpleDB.mdMgr();
		// tx = new Transaction();

		// Schema sch = new Schema();
		//
		// // sch.addDoubleField("yas");
		// sch.addIntField("yas");
		// // sch.addStringField("bosyer", 380);
		//
		// md.createTable("inputBtree", sch, tx);
		ti = md.getTableInfo(tblName, tx);

		// RecordFile rf = new RecordFile(ti, tx);
		// while (rf.next())
		// rf.delete();
		// rf.beforeFirst();
		// for (int id = 0; id < numberOfTuples; id++) {
		// rf.insert();
		// Random _rgen = new Random();
		// int fieldVal = _rgen.nextInt(200);
		// // int fieldVal = _rgen.nextInt(3);
		// // double fieldVal = _rgen.nextDouble()*200;
		// System.out.println("" + id + ".deðer: " + fieldVal);
		// rf.setInt("yas", fieldVal);
		// // rf.setDouble("yas", fieldVal);
		//
		// }

		// idxsch.addIntField("dataval");
		// // idxsch.addDoubleField("dataval");
		// idxsch.addIntField("block");
		// idxsch.addIntField("id");

		// Index idx = new BTreeIndex("btree", idxsch, tx);
		Plan p = new TablePlan(tblName, tx);
		UpdateScan rf1 = (UpdateScan) p.open();
		Index idx = null;

		// System.out.println("tablo adý:");
		// skyline olacak özellik/boyutlarýn herbiri için birer btree
		// oluþturuldu.
		for (String fieldName : selectedSkylineFields) {
			// bir skyline alaný için daha önce btree oluþturulmuþsa bir daha o
			// özellik için btree oluþturulmaz.
			// if (md.getTableInfo("bt" + fieldName + "leaf", tx).schema() ==
			// null && md.getTableInfo("bt" + fieldName + "dir", tx).schema() ==
			// null) {
			System.out.println("leaf ve dir oluþacak");
			if (ti.schema().type(fieldName) == INTEGER)
				idxsch.addIntField("dataval");
			if (ti.schema().type(fieldName) == DOUBLE)
				idxsch.addDoubleField("dataval");
			idxsch.addIntField("block");
			idxsch.addIntField("id");
			idx = new BTreeIndex("bt" + clickCount + fieldName, idxsch, tx);
			rf1.beforeFirst();
			// int f = 0;
			while (rf1.next()) {
				// System.out.println("" + f + ". eleman:" +
				// rf1.getVal(fieldName) + "rid:" + rf1.getRid());
				idx.insert(rf1.getVal(fieldName), rf1.getRid());
				// f++;
			}
			// }
			// else
			// System.out.println("leaf ve dir yok");
		}

		rf1.close();
		// if(idx!= null)
		idx.close();
		tx.commit();

	}

	public static boolean loadIndex(String TABLEname, String FIELDname, String IDXname, Transaction tx) {
		Map<String, IndexInfo> indexes = SimpleDB.mdMgr().getIndexInfo(TABLEname, tx);
		IndexInfo ii = indexes.get(FIELDname);
		if (ii != null) {

			System.out.println(IDXname + " already exist...");
			return false;

		}

		Plan p = new TablePlan(TABLEname, tx);
		UpdateScan s = (UpdateScan) p.open(); // UpdateScan is only for getRID

		Schema idxsch = new Schema();
		TableInfo ti;

		ti = SimpleDB.mdMgr().getTableInfo(TABLEname, tx);
		if (ti.schema().type(FIELDname) == INTEGER)
			idxsch.addIntField("dataval");
		if (ti.schema().type(FIELDname) == DOUBLE)
			idxsch.addDoubleField("dataval");

		idxsch.addIntField("block");
		idxsch.addIntField("id");
		Index idx = null;

		idx = new BTreeIndex(IDXname, idxsch, tx);

		System.out.println("loading " + IDXname + "...");
		while (s.next()) {
			idx.insert(s.getVal(FIELDname), s.getRid());
		}
		s.close();
		idx.close();

		SimpleDB.mdMgr().createIndex(IDXname, TABLEname, FIELDname, tx);
		return true;
	}
}
