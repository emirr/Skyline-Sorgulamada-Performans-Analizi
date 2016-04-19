package simpledb.index.btree;

import java.util.ArrayList;

import simpledb.record.Schema;
import simpledb.server.SimpleDB;
import simpledb.skyline.bnl.InitOneTable;

public class ExecutorForBtree {
	SkylineForBtree s4bt;

	public SkylineForBtree getS4bt() {
		return s4bt;
	}

	public ExecutorForBtree() {
		s4bt = new SkylineForBtree();
	}

	public void exec4Btree(int clickcount, int buffsize, String tblName, ArrayList<String> selectedSkylineFields) {
		// TODO Auto-generated method stub
		// int numberOfTuples = 5000;
		// SkylineForBtree s4bt ;
		// ExecutorForBtree(){
		// s4bt = new SkylineForBtree();
		// }
		// SimpleDB.init("btreedeneme6");
		// Schema sch = new Schema();

		// sch.addDoubleField("yas");
		// sch.addIntField("yas");
		// sch.addStringField("bosyer", 380);

		// md.createTable("inputBtree", sch, tx);
		// String tblName = "inputBtree3";
		// String dbName = "btreedeneme8";
		// String tblName = tablename;

		// Schema sch = new Schema();
		// sch.addIntField("uzaklik");
		// sch.addDoubleField("fiyat");
		// sch.addDoubleField("nufus");
		// sch.addIntField("binasayi");
		// InitOneTable init = new InitOneTable(numberOfTuples, "indp.",
		// tblName, sch, dbName, 10, 0);

		// InitOneTable init = new InitOneTable(tblName, sch);
		// tablomuz oluþtu.
		// ArrayList<String> selectedSkylineFields = new ArrayList<>();
		// selectedSkylineFields.add("uzaklik");
		// selectedSkylineFields.add("binasayi");
		// selectedSkylineFields.add("fiyat");
		// init.setSkylineFiels(selectedSkylineFields);
		// init.dummy();
		CreateBtree.bTreeCreate(tblName, selectedSkylineFields, clickcount);
		// Herbir skyline alaný için btree oluþturuldu.
		s4bt.findSkylineNominates(tblName, selectedSkylineFields, clickcount);
		s4bt.skylineFinder(clickcount, buffsize);

		//s4bt.print();

	}

}
