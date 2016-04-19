package simpledb.index.btree;

import static java.sql.Types.DOUBLE;
import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;

import java.util.ArrayList;
import java.util.HashMap;

import simpledb.metadata.MetadataMgr;
import simpledb.query.Constant;
import simpledb.query.Plan;
import simpledb.query.TablePlan;
import simpledb.query.UpdateScan;
import simpledb.record.RID;
import simpledb.record.RecordFile;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.skyline.bnl.BufferArranger;
import simpledb.skyline.bnl.InitOneTable;
import simpledb.tx.Transaction;

public class SkylineForBtree {
	ReadBtree rbt = new ReadBtree();
	// HashMap<RID,Constant> values;
	Transaction tx1, tx2;
	MetadataMgr md;
	TableInfo ti;
	Plan p;
	UpdateScan rf1;
	ArrayList<String> fieldlist = new ArrayList<>();
	HashMap<String, ArrayList<RID>> values = new HashMap<>();
	int recordnum;
	// HashSet<RID> skylineNominates = new HashSet<>();
	ArrayList<RID> skylineNominates = new ArrayList<>();
	ArrayList<Object> skylinePoints = new ArrayList<>();
	Constant currentval, nextval;
	int upsize;
	String tableName;
	ArrayList<String> selectedSkylineFields;
	BufferArranger buffArranger;
	public BufferArranger getBuffArranger() {
		return buffArranger;
	}

	//int[] existenceCount;// bir rid'nin kaç field içinde karþýlaþýldýðý
							// bilgisini tutar.
							// her bir indis,bir rid için bilgi tutar.Field
							// sayýsýna eþit olan ilk indis
							// skyline noktasýdýr.
	HashMap<RID,Integer> ridExistenceCount = new HashMap<>();
	// RID skylineRID;

	void findSkylineNominates(String tblname, ArrayList<String> slctdSkylineFields, int clickCount) {
		// SimpleDB.init("btreedeneme5");
		// tx1 = new Transaction();
		// md = SimpleDB.mdMgr();
		// ti = md.getTableInfo("inputBtree", tx1);
		// for (String fld : ti.schema().fields())
		// fieldlist.add(fld);//buradaki alanlar skyline alaný temsil eder.
		// tx1.commit();
		tableName = tblname;
		selectedSkylineFields = slctdSkylineFields;
		fieldlist = selectedSkylineFields;
		for (String fldname : fieldlist) {
			rbt.readInit(fldname, clickCount);
			values.put(fldname, rbt.getIds());
			System.out.println("values sayýsý:" +fldname +": " + values.get(fldname).size());
		}
		recordnum = InitOneTable.allTuples.length;
		//existenceCount = new int[recordnum];
		
		System.out.println("record sayýsý:" + recordnum);
		tx2 = new Transaction();
		md = SimpleDB.mdMgr();
		ti = md.getTableInfo(tblname, tx2);
		p = new TablePlan(tblname, tx2);
		rf1 = (UpdateScan) p.open();

		for (int i = 0; i < recordnum; i++) {
			for (String fldname : values.keySet()) {
				if (!skylineNominates.contains(values.get(fldname).get(i))) {
					System.out.println("eklenen rid:" + values.get(fldname).get(i));
					//int exstCount = existenceCount[i];
					//existenceCount[i] = 1;
					ridExistenceCount.put(values.get(fldname).get(i), 1);
					skylineNominates.add(values.get(fldname).get(i));
				
				} else {// bu kol hata verebilir.

					// if (i < (recordnum - 1)) {
					if (ridExistenceCount.get(values.get(fldname).get(i)) < (values.keySet().size() - 1)) {
						int exstCount = ridExistenceCount.get(values.get(fldname).get(i));
						//System.out.println("önce:" + exstCount);
						//existenceCount[i] = exstCount + 1;
						ridExistenceCount.put(values.get(fldname).get(i), ridExistenceCount.get(values.get(fldname).get(i)) + 1);
						//System.out.println("exstcount:" + existenceCount[i]);
						System.out.println("" + values.get(fldname).get(i) + " ridsinin görüldüðü özellik sayýsý:" + ridExistenceCount.get(values.get(fldname).get(i)));
					} else {
						// rf1.moveToRid(values.get(fldname).get(i));
						// currentval = rf1.getVal(fldname);
						// rf1.moveToRid(values.get(fldname).get(i + 1));
						// nextval = rf1.getVal(fldname);
						// if (currentval.equals(nextval)) {
						//
						// int k = i + 1;
						// while (currentval.equals(nextval) && k <
						// recordnum) {
						// //skylineNominates.contains(values.get(fldname).;
						// //
						// //if
						// (!skylineNominates.contains(values.get(fldname).get(k)))
						// if (existenceCount[k] ==
						// (values.keySet().size()))
						// skylineNominates.add(values.get(fldname).get(k));
						//
						// k++;
						// rf1.moveToRid(values.get(fldname).get(k));
						// nextval = rf1.getVal(fldname);
						// }
						// }
						// }
						System.out.println("" + values.get(fldname).get(i) + " ridsi skyline bundan sonrasýna bakma");
						i = recordnum;
						break;

					}
				}

				// i = recordnum;
				// break;
			}
			// }
		}
		rf1.close();
		tx2.commit();
	}

	// skyline adaylarý elimizde artýk skyline hesaplayabiliriz.
	void skylineFinder(int clickcount, int buffsize) {
		tx2 = new Transaction();
		md = SimpleDB.mdMgr();
		ti = md.getTableInfo(tableName, tx2);
		p = new TablePlan(tableName, tx2);
		rf1 = (UpdateScan) p.open();
		String typeBnl = "basic bnl";
		Schema skylineSch = new Schema();
		Schema sch = md.getTableInfo(tableName, tx2).schema();
		for (String skyfldname : selectedSkylineFields) {
			if (sch.type(skyfldname) == INTEGER) {
				System.out.println("skyline alan adý:" + skyfldname);
				skylineSch.addIntField(skyfldname);
			}
			if (sch.type(skyfldname) == DOUBLE) {
				System.out.println("skyline alan adý:" + skyfldname);
				skylineSch.addDoubleField(skyfldname);
			}

		}

		md.createTable("nom"+ clickcount + tableName, skylineSch, tx2);
		/*****/
		RecordFile rf2 = new RecordFile(md.getTableInfo("nom"+ clickcount + tableName, tx2), tx2);
		rf2.beforeFirst();
		//System.out.println("md creati halletmiþ olmalýsýn");
		// skylinePoints.add(skylineNominates.get(0));
		// int j=0;
		int listsize = skylineNominates.size();
		System.out.println("nominate size:" + listsize);
		// while(rf2.next()){
		for (int i = 0; i < listsize; i++) {
			rf2.insert();
			for (String schField : skylineSch.fields()) {
				//System.out.println(" ");
				//System.out.print("fieldname:" + schField + " ");
				if (sch.type(schField) == INTEGER) {
					rf1.moveToRid(skylineNominates.get(i));
					rf2.setInt(schField, rf1.getInt(schField));
					//System.out.print("" + rf2.getInt(schField) + " ");
				}
				if (sch.type(schField) == DOUBLE) {
					rf1.moveToRid(skylineNominates.get(i));
					rf2.setDouble(schField, rf1.getDouble(schField));
					//System.out.print("" + rf2.getDouble(schField) + " ");
					// System.out.println(" string yok ki");
				}
			}
		}
		rf1.close();
		rf2.close();
		tx2.commit();
		// System.out.println("nominate size:" + listsize);
		// //for (int i = 0; i < (listsize-1); i++)
		// int i = 0;
		//
		// while(i < (listsize-1)){
		// j = i + 1;
		// while( j < listsize){
		// if(compare4Domination(skylineNominates.get(i),
		// skylineNominates.get(j)) == -1){
		// skylineNominates.remove(i);
		// listsize = listsize - 1;
		// j = listsize;
		// i--;
		//
		// }
		// else{
		// if(compare4Domination(skylineNominates.get(i),
		// skylineNominates.get(j)) == 1){
		// skylineNominates.remove(j);
		// listsize = listsize - 1;
		// //j = skylineNominates.size();
		// //j++;
		// }
		// else{
		// j++;
		// }
		// }
		// }
		// i++;
		//
		// }
		/****/
		buffArranger = new BufferArranger(typeBnl, selectedSkylineFields, buffsize-3, "nom"+ clickcount + tableName, clickcount);
		// buffArranger = new BufferArranger(typeBnl, selectedSkylineFields,
		// buffSize - 3, tblName, clickCount);
		// long algorithmStartTime = System.currentTimeMillis();
		buffArranger.inputToWindow();
		// buffArranger.window.beforeFirst();
		// while(buffArranger.window.next()){
		// System.out.println(" " + buffArranger.window.getInt("A"));
		// System.out.println(" " + buffArranger.window.getInt("B"));
		// }
		buffArranger.readFromTemp();
		// System.out.println("elde edilen skyline");
		// for(int i=0; i < skylineNominates.size(); i++){
		// System.out.println("");
		// }
		// createSkylineInfo();

		// tx2.commit();

	}

	// void createSkylineInfo() {
	// for (RID rid : skylineNominates) {
	// rf1.moveToRid(rid);
	// for (String tempField : fieldlist) {
	// if (ti.schema().type(tempField) == INTEGER)
	// skylinePoints.add(rf1.getInt(tempField));
	// if (ti.schema().type(tempField) == DOUBLE)
	// skylinePoints.add(rf1.getDouble(tempField));
	//
	// }
	// }
	// }
	
	public void print() {
		System.out.println("" + buffArranger.getSkyline().getSkylinePoints().size() / selectedSkylineFields.size()
				+ " skyline bulundu.");
		int indx = 0;
		for (Object wind : buffArranger.getSkyline().getSkylinePoints()) {
			// System.out.println(" " + wind.SIZE);
			if (indx < selectedSkylineFields.size()) {
				if (wind instanceof Integer)
					System.out.print(" " + (int) wind);
				if (wind instanceof Double)
					System.out.print(" " + (double) wind);
				indx++;
				// System.out.println("indx" + indx);
			} else {
				System.out.println();
				indx = 0;
				indx++;
				// System.out.println("indxelse" + indx);
				if (wind instanceof Integer)
					System.out.print(" " + (int) wind);
				if (wind instanceof Double)
					System.out.print(" " + (double) wind);
				// System.out.print(" " + wind);
				// System.out.println(" ");
			}
		}
	}

	// int compare4Domination(RID selectedRID, RID otherRID) {
	// rf1.beforeFirst();
	// int k = 0;
	// upsize = fieldlist.size();
	//
	// Object selected = null;
	// Object other = null;
	//
	// for (String tempField : fieldlist) {
	// // System.out.println("tempfield:" + tempField + "alan tipi:" +
	// // sch.type(tempField));
	// // System.out.println("" + input.getInt(tempField));
	// if (ti.schema().type(tempField) == INTEGER) {
	// // System.out.println("input alan:" + tempField + " " +
	// // input.getInt(tempField));
	// // System.out.println("window alan:" + tempField + " " +
	// // window.getInt(tempField));
	// rf1.moveToRid(selectedRID);
	// selected = rf1.getInt(tempField);
	// rf1.moveToRid(otherRID);
	// other = rf1.getInt(tempField);
	// k += deepCompare(selected, other);
	//
	// }
	// if (ti.schema().type(tempField) == DOUBLE) {
	// rf1.moveToRid(selectedRID);
	// selected = rf1.getDouble(tempField);
	// rf1.moveToRid(otherRID);
	// other = rf1.getDouble(tempField);
	// k += deepCompare(selected, other);
	// }
	//
	// }
	// if ((upsize != 0) && k == upsize)
	// return 1;
	// else {
	// if ((upsize != 0) && k == -upsize)
	// return -1;
	// else
	// return 0;
	// }
	//
	// }
	//
	// public int deepCompare(Object x, Object y) {
	// int m = -2;
	// if (x instanceof Integer && y instanceof Integer) {
	// if ((int) x < (int) y)
	// m = 1;
	// else {
	// if ((int) x > (int) y)
	// m = -1;
	// else {
	// upsize--;
	// m = 0;
	// }
	//
	// }
	// }
	// if (x instanceof Double && y instanceof Double) {
	// if ((double) x < (double) y)
	// m = 1;
	// else {
	// if ((double) x > (double) y)
	// m = -1;
	// else {
	// upsize--;
	// m = 0;
	// }
	//
	// }
	// }
	// return m;
	// }
}
