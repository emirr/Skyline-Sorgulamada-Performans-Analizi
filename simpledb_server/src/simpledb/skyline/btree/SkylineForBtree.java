package simpledb.skyline.btree;

import static java.sql.Types.DOUBLE;
import static java.sql.Types.INTEGER;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import simpledb.file.FileMgr;
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
	Schema sch;
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
	ArrayList<ArrayList<Object>> allSkylinePoints = new ArrayList<ArrayList<Object>>();
	ArrayList<Object> oneSkylinePoint = new ArrayList<>();
	ArrayList<Object> skylinePoints2 = new ArrayList<>();
	public ArrayList<Object> getSkylinePoints2() {
		return skylinePoints2;
	}

	Constant currentval, nextval;
	int upsize;
	private RID firstSkylineRid;
	String tableName;
	ArrayList<String> selectedSkylineFields;
	BufferArranger buffArranger;
	HashMap<String, ReadBtree> readBTreeobjects = new HashMap<>();
	

	public BufferArranger getBuffArranger() {
		return buffArranger;
	}

	// int[] existenceCount;// bir rid'nin kaç field içinde karþýlaþýldýðý
	// bilgisini tutar.
	// her bir indis,bir rid için bilgi tutar.Field
	// sayýsýna eþit olan ilk indis
	// skyline noktasýdýr.
	HashMap<RID, Integer> ridExistenceCount = new HashMap<>();
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

		
		// for (String fldname : fieldlist) {
		// rbt.readInit(fldname, clickCount);
		//
		// values.put(fldname, rbt.getIds());
		// System.out.println("values sayýsý:" +fldname +": " +
		// values.get(fldname).size());
		// }
		for (String fldname : fieldlist) {
			readBTreeobjects.put(fldname, new ReadBtree());
		}
		// for (String fldname : fieldlist) {
		// rbt.readInit(fldname, clickCount);
		// values.put(fldname, rbt.getIds());
		// System.out.println("values sayýsý:" +fldname +": " +
		// values.get(fldname).size());
		// }
		recordnum = InitOneTable.allTuples.length;
		int diskErisimSayýsý = (FileMgr.getReadCount() + FileMgr.getWriteCount());
		for (int i = 0; i < recordnum; i++) {
			for (String fldname : fieldlist) {
				RID rid1 = readBTreeobjects.get(fldname).readSync(fldname, clickCount, i);
				if (!skylineNominates.contains(rid1))
				{
					//System.out.println("eklenen rid:" + rid1);
					// int exstCount = existenceCount[i];
					// existenceCount[i] = 1;
					ridExistenceCount.put(rid1, 1);
					skylineNominates.add(rid1);
				

				}
				else{
					if (ridExistenceCount.get(rid1) < (fieldlist.size() - 1)) {
						int exstCount = ridExistenceCount.get(rid1);
						//System.out.println("önce:" + exstCount);
						// existenceCount[i] = exstCount + 1;
						ridExistenceCount.put(rid1,
								ridExistenceCount.get(rid1) + 1);
						// System.out.println("exstcount:" + existenceCount[i]);
						//System.out.println("" + rid1 + " ridsinin görüldüðü özellik sayýsý:"
							//	+ ridExistenceCount.get(rid1));
					}
					else{
						System.out.println("" + rid1 + " ridsi skyline bundan sonrasýna bakma");
						firstSkylineRid = rid1;
						i = recordnum;
						System.out.println("þimdi okuma bitecek :" + recordnum);
						for(String fieldname: fieldlist)
							readBTreeobjects.get(fieldname).readSync(fldname, clickCount, recordnum);
						break;
					}
				}

			}
		}
		int diskErisimSayýsý2 = (FileMgr.getReadCount() + FileMgr.getWriteCount());
		System.out.println("nominate bulmak için disk eriþim sayýsý:" + (diskErisimSayýsý2 - diskErisimSayýsý));

		// recordnum = InitOneTable.allTuples.length;
		// existenceCount = new int[recordnum];

		System.out.println("record sayýsý:" + recordnum);

	}

	// skyline adaylarý elimizde artýk skyline hesaplayabiliriz.
	void skylineFinder(int clickcount, int buffsize) {
		int disk19 = FileMgr.getReadCount();
		tx2 = new Transaction();
		md = SimpleDB.mdMgr();
		ti = md.getTableInfo(tableName, tx2);
		System.out.println("tablo adý:" + tableName);
		p = new TablePlan(tableName, tx2);
		rf1 = (UpdateScan) p.open();
		//String typeBnl = "basic bnl";
		Schema skylineSch = new Schema();
		sch = md.getTableInfo(tableName, tx2).schema();
		for (String skyfldname : selectedSkylineFields) {
			if (sch.type(skyfldname) == INTEGER) {
				//System.out.println("skyline alan adý:" + skyfldname);
				skylineSch.addIntField(skyfldname);
			}
			if (sch.type(skyfldname) == DOUBLE) {
				//System.out.println("skyline alan adý:" + skyfldname);
				skylineSch.addDoubleField(skyfldname);
			}

		}

		int presize = skylineNominates.size();
		//System.out.println("presize:"+presize);
		skylineNominates.remove(firstSkylineRid);
		int listsize = skylineNominates.size();
		System.out.println("nominate size:" + listsize);
		
		//int diskErisimSayýsý = (FileMgr.getReadCount() + FileMgr.getWriteCount());
		// while(rf2.next()){
		rf1.moveToRid(firstSkylineRid);
		for (String schField : skylineSch.fields()) {
			if (sch.type(schField) == INTEGER) {
				//System.out.println("gidilecek id:" + skylineNominates.get(i));
				oneSkylinePoint.add(rf1.getInt(schField));
			}
			if (sch.type(schField) == DOUBLE) {
				//rf1.moveToRid(firstSkylineRid);
				oneSkylinePoint.add(rf1.getDouble(schField));
				// System.out.print("" + rf2.getDouble(schField) + " ");
				// System.out.println(" string yok ki");
			}
		}
		
//		System.out.println(""+allSkylinePoints.size()+". kayýt eklenecek");
//		for(int h=0; h<oneSkylinePoint.size();h++){
//			System.out.print("-"+oneSkylinePoint.get(h));
//		}
//		System.out.println("");
		allSkylinePoints.add(oneSkylinePoint);
		for (int i = 0; i < listsize; i++) {
			//rf2.insert();
			rf1.moveToRid(skylineNominates.get(i));
			ArrayList<Object> otherOneSkyline = new ArrayList<>();

			if(!isDominatedInSet()){
				for (String schField : skylineSch.fields()) {
					if (sch.type(schField) == INTEGER) {
						//System.out.println("gidilecek id:" + skylineNominates.get(i));
						otherOneSkyline.add(rf1.getInt(schField));
					}
					if (sch.type(schField) == DOUBLE) {
						//rf1.moveToRid(firstSkylineRid);
						otherOneSkyline.add(rf1.getDouble(schField));
						// System.out.print("" + rf2.getDouble(schField) + " ");
						// System.out.println(" string yok ki");
					}
				}
//				System.out.print(""+allSkylinePoints.size()+". kayýt eklenecek eklenen rid:"+skylineNominates.get(i)+"deðerler: ");
//				for(int h=0; h<otherOneSkyline.size();h++){
//					System.out.print("-"+otherOneSkyline.get(h));
//				}
			//	System.out.println("");
				int dominateStatIndex = isDominatesAnySetElement();
				if(dominateStatIndex>-1)
					allSkylinePoints.set(dominateStatIndex, otherOneSkyline);
				
				else
					allSkylinePoints.add(otherOneSkyline);
			}

			
		}
		int diskErisimSayýsý2 = FileMgr.getReadCount();

		rf1.close();
	//	rf2.close();
		tx2.commit();
		int dsk = diskErisimSayýsý2 - disk19;
		System.out.println("ana dosyadan btree için deðer okuma disk eriþim sayýsý:"+dsk);
		

}
	public int isDominatesAnySetElement(){
		for (int i=0; i<allSkylinePoints.size();i++){
			if (compareDomination(allSkylinePoints.get(i)) == 1) {
				return i;
			}
		}
		return -1;
	}
	public int compareDomination(ArrayList<Object> oneSkylinePoint1) {

		int k = 0;
		int i = 0;
		upsize = selectedSkylineFields.size();
		for (String tempField : selectedSkylineFields) {

			if (sch.type(tempField) == INTEGER){
				k += deepCompare(rf1.getInt(tempField), oneSkylinePoint1.get(i));
			}
			if (sch.type(tempField) == DOUBLE)
				k += deepCompare(rf1.getDouble(tempField), oneSkylinePoint1.get(i));
			
			i++;
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

	public int deepCompare(Object x, Object y) {
		int m = -2;
		if (x instanceof Integer && y instanceof Integer) {
			if ((int) x < (int) y)
				m = 1;
			else {
				if ((int) x > (int) y)
					m = -1;
				else {
					upsize--;
					m = 0;
				}

			}
		}
		if (x instanceof Double && y instanceof Double) {
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
		}
		return m;
	}
	private boolean isDominatedInSet() {
		for (ArrayList<Object> oneSkylinePoint1: allSkylinePoints)
			if (compareDomination(oneSkylinePoint1) == -1) {
				return true;
			}
		return false;
	}
	public void print() {
		
		System.out.println("bulunan skyline sayýsý:" + allSkylinePoints.size());
		for(int i=0; i< allSkylinePoints.size();i++){
			for(int j= 0; j<allSkylinePoints.get(i).size(); j++){
				skylinePoints2.add(allSkylinePoints.get(i).get(j));
			}
		}
		int indx = 0;
//		for (Object wind : skylinePoints2) {
//			// System.out.println(" " + wind.SIZE);
//			if (indx < selectedSkylineFields.size()) {
//				if (wind instanceof Integer)
//					System.out.print(" " + (int) wind);
//				if (wind instanceof Double)
//					System.out.print(" " + (double) wind);
//				indx++;
//				// System.out.println("indx" + indx);
//			} else {
//				System.out.println();
//				indx = 0;
//				indx++;
//				// System.out.println("indxelse" + indx);
//				if (wind instanceof Integer)
//					System.out.print(" " + (int) wind);
//				if (wind instanceof Double)
//					System.out.print(" " + (double) wind);
//				// System.out.print(" " + wind);
//				 System.out.println(" ");
//			}
//		}
		
		for(String fldname : selectedSkylineFields){
			System.out.print(" "+fldname+"aðacýnda gezilen index node sayýsý:"+ExecutorForBtree.heightForEachTree.get(fldname));
			System.out.print(" "+fldname+"aðacýnda gezilen leaf node sayýsý:"+readBTreeobjects.get(fldname).getVisitedLeafNodeCountForOneFieldTree().get(fldname));
			if(readBTreeobjects.get(fldname).getVisitedOFNodeCountForOneFieldTree().get(fldname) != null)
				System.out.print(" gezilen of node sayýsý:" + readBTreeobjects.get(fldname).getVisitedOFNodeCountForOneFieldTree().get(fldname));
		
				
			System.out.println(" ");
		}
	}

}
