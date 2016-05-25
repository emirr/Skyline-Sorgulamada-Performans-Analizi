package simpledb.skyline.bnl;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

import simpledb.file.FileMgr;
import simpledb.record.Schema;
import simpledb.skyline.btree.CreateBtree;
import simpledb.skyline.btree.ExecutorForBtree;
import simpledb.skyline.gui.GraphicTest;
import simpledb.skyline.rtree.BBSExecutor;
import simpledb.tx.Transaction;

public class ExecutorOfAllSystem {

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
	BBSExecutor rtreeexec;
	InitOneTable init;

	public ExecutorOfAllSystem(Schema sch, ArrayList<String> skyFld) {
		this.sch = sch;
		this.selectedSkylineFields = skyFld;
	}

	public ExecutorOfAllSystem(Schema sch) {
		// System.out.println("þema tamam mý??");
		this.sch = sch;
		for (String sch1 : sch.fields())
			System.out.println("þema bilgisi in test1:" + sch1);
	}

	public void createSystem(int tupleSize, String distributionType, String tblName, String dbName, int buffSize,
			int tableCount, HashMap<String, Boolean> fieldIndexStat) {

		init = new InitOneTable(tupleSize, distributionType, tblName, sch, dbName, buffSize, tableCount);
		Transaction txnew = new Transaction();
		// if(algType == "btree"){
		for (String field : sch.fields()) {
			if(fieldIndexStat.get(field) == true){
				CreateBtree.loadIndex(tblName, field, "bt" + field, txnew);
			}
			
		}
		// }
		txnew.commit();
		// if(init != null)
		// System.out.println("init mall");
		// init.dummy();
	}

	public void execCurrentSystem(String tblName, int bufferSize, String typeBnl, int clickCount, String dbName) {
		// System.out.println("dbname:" + dbName);
		// String dirName = "C:\\Users\\oblmv2" + "\\" + dbName;
		// System.out.println(dirName);
		// File dir = new File(dirName);
		//
		// for (String filename : dir.list()) {
		// //System.out.println("filename:" + filename);
		// if (filename.startsWith("bt") || filename.startsWith("nom")) {
		// new File(dir, filename).delete();
		// System.out.println("silinen file:" + filename);
		// }
		// }

		init = new InitOneTable(tblName, sch);
		init.setSkylineFiels(getSelectedSkylineFields());
		init.dummy();
		// diskErisimSayýsý = (FileMgr.getReadCount() +
		// FileMgr.getWriteCount());
		if (typeBnl == "btree") {
			execBtree = new ExecutorForBtree(dbName);
			//diskErisimSayýsý = (FileMgr.getReadCount() + FileMgr.getWriteCount());
			algorithmStartTime = System.currentTimeMillis();
			Runtime runtime1 = Runtime.getRuntime();
			execBtree.exec4Btree(clickCount, bufferSize, tblName, selectedSkylineFields);
			System.out.println("toplam bellek:"+runtime1.totalMemory() / 1000000 +" MB");
			System.out.println("kullanýlan bellek:"+(runtime1.totalMemory() - runtime1.freeMemory()) / 1000000 +" MB");
			System.out.println("bellek kullaným oraný:" + ((runtime1.totalMemory() - runtime1.freeMemory())/runtime1.totalMemory())*100);
			algorithmStopTime = System.currentTimeMillis();
			//System.out.println("önceki disk eriþim sayýsý:" + diskErisimSayýsý);
			//diskErisimSayýsý2 = (FileMgr.getReadCount() + FileMgr.getWriteCount());

			//System.out.println("sonraki disk eriþim sayýsý:" + diskErisimSayýsý2);
			//System.out.println("hesap disk eriþim sayýsý:" + (diskErisimSayýsý2 - diskErisimSayýsý));

		//	System.out.println("dosya iþlemlerinden sonra elde edilen skyline:");
			execBtree.getS4bt().print();

			long processTime = algorithmStopTime - algorithmStartTime;

			System.out.println(
					"algoritmanýn iþlem süresi:" + processTime / 60000 + " dakika " + (processTime % 60) + " saniye");
			GraphicTest gt = new GraphicTest(InitOneTable.allTuples,
					execBtree.getS4bt().getSkylinePoints2());

		} else if(typeBnl != "rtree"){
			Runtime runtime2 = Runtime.getRuntime();
			buffArranger = new BufferArranger(dbName, typeBnl, selectedSkylineFields, bufferSize - 3, tblName, clickCount);
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
			System.out.println("toplam bellek:"+runtime2.totalMemory() / 1000000 +" MB");
			System.out.println("kullanýlan bellek:"+(runtime2.totalMemory() - runtime2.freeMemory()) / 1000000 +" MB");
			System.out.println("bellek kullaným oraný:" + ((runtime2.totalMemory() - runtime2.freeMemory())/runtime2.totalMemory())*100);
			System.out.println("disk kullanum alaný:"+ buffArranger.extraStrorageInfo()/1024 +"KB" );
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
		}else{
//			if(clickCount == 0){
//				String treeDir = "A:\\bitirme_workspace\\git\\simpledb_server";
//				File dir = new File(treeDir);
//
//				for (String filename : dir.list()){
//			         if (filename.startsWith("tmp_rtree"))
//			         new File(treeDir, filename).delete();
//				}
//			}
			rtreeexec = new BBSExecutor(selectedSkylineFields);
			rtreeexec.executeAllTreeOps(tblName,10,clickCount);
			
			GraphicTest gt = new GraphicTest(rtreeexec.getAllTuples(), rtreeexec.getSkylinePoints());

		}
	}

	public void execNewSystem(String typeBnl, int buffSize, String tblName, int tupleSize, String distributionType,
			String dbName, int clickCount) {
		// TODO Auto-generated method stub
		// String typeOfBnl = typeBnl;
		// int windSize = winSize;
		// String tableName = tblName;
		// String dirName = "C:\\Users\\oblmv2" + "\\" + dbName;
		// System.out.println(dirName);
		// File dir = new File(dirName);
		//
		// for (String filename : dir.list()) {
		// System.out.println("filename:" + filename);
		// if (filename.startsWith("bt") || filename.startsWith("nom")) {
		// new File(dir, filename).delete();
		// System.out.println("silinen file:" + filename);
		// }
		// }

		init = new InitOneTable(tblName, sch);
		/* bnl,windsize,tableName burada atanacak. */

		// init = new InitOneTable(tupleSize, distributionType, tblName, sch,
		// dbName, buffSize,clickCount);
		if (init == null)
			System.out.println("init null");
		init.setSkylineFiels(getSelectedSkylineFields());

		init.dummy();
		if (typeBnl == "btree") {
			execBtree = new ExecutorForBtree(dbName);
			//diskErisimSayýsý = (FileMgr.getReadCount() + FileMgr.getWriteCount());
			algorithmStartTime = System.currentTimeMillis();
			Runtime runtime3 = Runtime.getRuntime();
			execBtree.exec4Btree(clickCount, buffSize, tblName, selectedSkylineFields);
			System.out.println("toplam bellek:"+runtime3.totalMemory() / 1000000 +" MB");
			System.out.println("kullanýlan bellek:"+(runtime3.totalMemory() - runtime3.freeMemory()) / 1000000 +" MB");
			System.out.println("bellek kullaným oraný:" + ((runtime3.totalMemory() - runtime3.freeMemory())/runtime3.totalMemory())*100);

			algorithmStopTime = System.currentTimeMillis();
			//System.out.println("önceki disk eriþim sayýsý:" + diskErisimSayýsý);
			//diskErisimSayýsý2 = (FileMgr.getReadCount() + FileMgr.getWriteCount());

			//System.out.println("sonraki disk eriþim sayýsý:" + diskErisimSayýsý2);
			//System.out.println("hesap disk eriþim sayýsý:" + (diskErisimSayýsý2 - diskErisimSayýsý));

			System.out.println("dosya iþlemlerinden sonra elde edilen skyline:");
			execBtree.getS4bt().print();
			

			long processTime = algorithmStopTime - algorithmStartTime;

			System.out.println(
					"algoritmanýn iþlem süresi:" + processTime / 60000 + " dakika " + (processTime % 60) + " saniye");
			GraphicTest gt = new GraphicTest(InitOneTable.allTuples,
					execBtree.getS4bt().getSkylinePoints2());

		} else if(typeBnl != "rtree"){
			Runtime runtime4 = Runtime.getRuntime();

			buffArranger = new BufferArranger(dbName,typeBnl, selectedSkylineFields, buffSize - 3, tblName, clickCount);
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
			System.out.println("toplam bellek:"+runtime4.totalMemory() / 1000000 +" MB");
			System.out.println("kullanýlan bellek:"+(runtime4.totalMemory() - runtime4.freeMemory()) / 1000000 +" MB");
			System.out.println("bellek kullaným oraný:" + ((runtime4.totalMemory() - runtime4.freeMemory())/runtime4.totalMemory())*100);
			System.out.println("disk kullanum alaný:"+ buffArranger.extraStrorageInfo()/1024 +"KB" );
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

		}else{
			if(clickCount == 0){
				String treeDir = "A:\\bitirme_workspace\\git\\simpledb_server";
				File dir = new File(treeDir);

				for (String filename : dir.list()){
			         if (filename.startsWith("tmp_rtree"))
			         new File(treeDir, filename).delete();
				}
			}
			rtreeexec = new BBSExecutor(selectedSkylineFields);
			rtreeexec.executeAllTreeOps(tblName,10,clickCount);
			GraphicTest gt = new GraphicTest(rtreeexec.getAllTuples(), rtreeexec.getSkylinePoints());

		}

	}

}
