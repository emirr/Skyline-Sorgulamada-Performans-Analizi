package simpledb.skyline.bnl;

import java.io.File;
import java.util.ArrayList;

import simpledb.file.FileMgr;
import simpledb.index.btree.ExecutorForBtree;
import simpledb.record.Schema;

public class Test1 {

	/**
	 * @param args
	 */
	ArrayList<String> selectedSkylineFields;

	public ArrayList<String> getSelectedSkylineFields() {
		return selectedSkylineFields;
	}

	public void setSelectedSkylineFields(ArrayList<String> selectedSkylineFields) {
		this.selectedSkylineFields = selectedSkylineFields;
		// buffArranger.setSkylineBoyut(selectedSkylineFields);

	}

	Schema sch;
	long algorithmStartTime, algorithmStopTime;
	int diskErisimSayýsý, diskErisimSayýsý2;
	// String algoType;
	// int windowSize;
	BufferArranger buffArranger;
	ExecutorForBtree execBtree;
	InitOneTable init;

	public Test1(Schema sch, ArrayList<String> skyFld) {
		this.sch = sch;
		this.selectedSkylineFields = skyFld;
	}

	public Test1(Schema sch) {
		// System.out.println("þema tamam mý??");
		this.sch = sch;
		for (String sch1 : sch.fields())
			System.out.println("þema bilgisi in test1:" + sch1);
	}

	public void createSystem(int tupleSize, String distributionType, String tblName, String dbName, int buffSize,
			int tableCount) {
		
		init = new InitOneTable(tupleSize, distributionType, tblName, sch, dbName, buffSize, tableCount);
		// if(init != null)
		// System.out.println("init mall");
		// init.dummy();
	}

	public void execCurrentSystem(String tblName, int bufferSize, String typeBnl, int clickCount, String dbName) {
//		System.out.println("dbname:" + dbName);
//		String dirName = "C:\\Users\\oblmv2" + "\\" + dbName;
//		System.out.println(dirName);
//		File dir = new File(dirName);
//
//		for (String filename : dir.list()) {
//			//System.out.println("filename:" + filename);
//			if (filename.startsWith("bt") || filename.startsWith("nom")) {
//				new File(dir, filename).delete();
//				System.out.println("silinen file:" + filename);
//			}
//		}

		init = new InitOneTable(tblName, sch);
		init.setSkylineFiels(getSelectedSkylineFields());
		init.dummy();
		// diskErisimSayýsý = (FileMgr.getReadCount() +
		// FileMgr.getWriteCount());
		if (typeBnl == "btree") {
			execBtree = new ExecutorForBtree();
			diskErisimSayýsý = (FileMgr.getReadCount() + FileMgr.getWriteCount());
			algorithmStartTime = System.currentTimeMillis();
			execBtree.exec4Btree(clickCount, bufferSize, tblName, selectedSkylineFields);
			algorithmStopTime = System.currentTimeMillis();
			System.out.println("önceki disk eriþim sayýsý:" + diskErisimSayýsý);
			diskErisimSayýsý2 = (FileMgr.getReadCount() + FileMgr.getWriteCount());

			System.out.println("sonraki disk eriþim sayýsý:" + diskErisimSayýsý2);
			System.out.println("hesap disk eriþim sayýsý:" + (diskErisimSayýsý2 - diskErisimSayýsý));

			System.out.println("dosya iþlemlerinden sonra elde edilen skyline:");
			execBtree.getS4bt().print();

			long processTime = algorithmStopTime - algorithmStartTime;

			System.out.println(
					"algoritmanýn iþlem süresi:" + processTime / 60000 + " dakika " + (processTime % 60) + " saniye");
			GraphicTest gt = new GraphicTest(InitOneTable.allTuples,
					execBtree.getS4bt().getBuffArranger().getSkyline().getSkylinePoints());

		} else {
			buffArranger = new BufferArranger(typeBnl, selectedSkylineFields, bufferSize - 3, tblName, clickCount);
			// buffArranger = new BufferArranger(typeBnl, selectedSkylineFields,
			// buffSize - 3, tblName, clickCount);
			diskErisimSayýsý = (FileMgr.getReadCount() + FileMgr.getWriteCount());
			algorithmStartTime = System.currentTimeMillis();
			buffArranger.inputToWindow();
			// buffArranger.window.beforeFirst();
			// while(buffArranger.window.next()){
			// System.out.println(" " + buffArranger.window.getInt("A"));
			// System.out.println(" " + buffArranger.window.getInt("B"));
			// }
			buffArranger.readFromTemp();
			algorithmStopTime = System.currentTimeMillis();

			System.out.println("önceki disk eriþim sayýsý:" + diskErisimSayýsý);
			diskErisimSayýsý2 = (FileMgr.getReadCount() + FileMgr.getWriteCount());

			System.out.println("sonraki disk eriþim sayýsý:" + diskErisimSayýsý2);
			System.out.println("hesap disk eriþim sayýsý:" + (diskErisimSayýsý2 - diskErisimSayýsý));
			System.out.println("dosya iþlemlerinden sonra elde edilen skyline:");

			System.out.println("skyline point size:" + buffArranger.getSkyline().getSkylinePoints().size());
			System.out.println("skylinefield size:" + init.getSkylineFields().size());
			System.out.println("" + buffArranger.getSkyline().getSkylinePoints().size() / init.getSkylineFields().size()
					+ " skyline bulundu.");
			int indx = 0;
			for (Object wind : buffArranger.getSkyline().getSkylinePoints()) {
				// System.out.println(" " + wind.SIZE);
				if (indx < init.getSkylineFields().size()) {
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
			long processTime = algorithmStopTime - algorithmStartTime;

			System.out.println(
					"algoritmanýn iþlem süresi:" + processTime / 60000 + " dakika " + (processTime % 60) + " saniye");
			GraphicTest gt = new GraphicTest(InitOneTable.allTuples, buffArranger.getSkyline().getSkylinePoints());
		}
	}

	public void execNewSystem(String typeBnl, int buffSize, String tblName, int tupleSize, String distributionType,
			String dbName, int clickCount) {
		// TODO Auto-generated method stub
		// String typeOfBnl = typeBnl;
		// int windSize = winSize;
		// String tableName = tblName;
//		String dirName = "C:\\Users\\oblmv2" + "\\" + dbName;
//		System.out.println(dirName);
//		File dir = new File(dirName);
//		
//		for (String filename : dir.list()) {
//			System.out.println("filename:" + filename);
//			if (filename.startsWith("bt") || filename.startsWith("nom")) {
//				new File(dir, filename).delete();
//				System.out.println("silinen file:" + filename);
//			}
//		}

		init = new InitOneTable(tblName, sch);
		/* bnl,windsize,tableName burada atanacak. */

		// init = new InitOneTable(tupleSize, distributionType, tblName, sch,
		// dbName, buffSize,clickCount);
		if (init == null)
			System.out.println("init null");
		init.setSkylineFiels(getSelectedSkylineFields());

		init.dummy();
		if (typeBnl == "btree") {
			execBtree = new ExecutorForBtree();
			diskErisimSayýsý = (FileMgr.getReadCount() + FileMgr.getWriteCount());
			algorithmStartTime = System.currentTimeMillis();
			execBtree.exec4Btree(clickCount, buffSize, tblName, selectedSkylineFields);
			algorithmStopTime = System.currentTimeMillis();
			System.out.println("önceki disk eriþim sayýsý:" + diskErisimSayýsý);
			diskErisimSayýsý2 = (FileMgr.getReadCount() + FileMgr.getWriteCount());

			System.out.println("sonraki disk eriþim sayýsý:" + diskErisimSayýsý2);
			System.out.println("hesap disk eriþim sayýsý:" + (diskErisimSayýsý2 - diskErisimSayýsý));

			System.out.println("dosya iþlemlerinden sonra elde edilen skyline:");
			execBtree.getS4bt().print();

			long processTime = algorithmStopTime - algorithmStartTime;

			System.out.println(
					"algoritmanýn iþlem süresi:" + processTime / 60000 + " dakika " + (processTime % 60) + " saniye");
			GraphicTest gt = new GraphicTest(InitOneTable.allTuples,
					execBtree.getS4bt().getBuffArranger().getSkyline().getSkylinePoints());

		} else {
			buffArranger = new BufferArranger(typeBnl, selectedSkylineFields, buffSize - 3, tblName, clickCount);
			diskErisimSayýsý = (FileMgr.getReadCount() + FileMgr.getWriteCount());
			System.out.println("önceki disk eriþim sayýsý:" + diskErisimSayýsý);
			long algorithmStartTime = System.currentTimeMillis();
			buffArranger.inputToWindow();
			// buffArranger.window.beforeFirst();
			// while(buffArranger.window.next()){
			// System.out.println(" " + buffArranger.window.getInt("A"));
			// System.out.println(" " + buffArranger.window.getInt("B"));
			// }
			buffArranger.readFromTemp();
			long algorithmStopTime = System.currentTimeMillis();
			// buffArranger.getInput().close();
			diskErisimSayýsý2 = (FileMgr.getReadCount() + FileMgr.getWriteCount());
			System.out.println("sonraki disk eriþim sayýsý:" + diskErisimSayýsý2);
			System.out.println("hesap disk eriþim sayýsý:" + (diskErisimSayýsý2 - diskErisimSayýsý));

			System.out.println("dosya iþlemlerinden sonra elde edilen skyline:");

			System.out.println("" + buffArranger.getSkyline().getSkylinePoints().size() / init.getSkylineFields().size()
					+ " skyline bulundu.");
			int indx = 0;
			for (Object wind : buffArranger.getSkyline().getSkylinePoints()) {
				// System.out.println(" " + wind.SIZE);
				if (indx < init.getSkylineFields().size()) {
					// System.out.print(" " + wind);
					indx++;
					if (wind instanceof Integer)
						System.out.print(" " + (int) wind);
					if (wind instanceof Double)
						System.out.print(" " + (double) wind);
				} else {
					System.out.println();
					indx = 0;
					indx++;
					if (wind instanceof Integer)
						System.out.print(" " + (int) wind);
					if (wind instanceof Double)
						System.out.print(" " + (double) wind);
					// System.out.print(" " + wind);
					// System.out.println(" ");
				}
			}
			long processTime = algorithmStopTime - algorithmStartTime;

			System.out.println(
					"algoritmanýn iþlem süresi:" + processTime / 60000 + " dakika " + (processTime % 60) + " saniye");
			/*
			 * 52-57 arasý gui'ye gerekli deðerleri göndermek için aksi halde
			 * her seferinde yeni bir input table üretmek zorunda kalýrýz.
			 */
			// int [][] skylines = new
			// int[SkylineFinder.skylinePoints.size()/init.getSkylineFields().size()][init.getSkylineFields().size()];
			// for(int i = 0; i <
			// (SkylineFinder.skylinePoints.size()/init.getSkylineFields().size());
			// i++){
			// for(int u = 0; u < init.getSkylineFields().size(); u++){
			// skylines[i][u] = SkylineFinder.skylinePoints.get(*i);
			// //skylines[i][1] = SkylineFinder.skylinePoints.get(2*i+1);
			// }
			//
			// }
			// System.out.println("skyline size in test1:" + skylines.length);
			GraphicTest gt = new GraphicTest(InitOneTable.allTuples, buffArranger.getSkyline().getSkylinePoints());

		}

	}

}
