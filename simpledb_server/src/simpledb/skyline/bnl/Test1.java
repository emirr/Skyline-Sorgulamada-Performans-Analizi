package simpledb.skyline.bnl;

import java.util.ArrayList;

import simpledb.file.FileMgr;
import simpledb.query.TableScan;
import simpledb.record.Schema;
import simpledb.record.TableInfo;

/*
 * Bu testin amacý 8 adet tampon bölgesinin koordinasyonunu anlamaktir. Tamponumuz 3 kýsma ayrýldi.
 * 1 tampon: input dosyasýndan gelen kayýtlar
 * 5 tampon: window bölgemiz.
 * 2 tampon: tempfile'a yazana output buffer'dir. Tempfile'in geniþlemesi için auni anda 2 tampona ihtiyaç var.
 */

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
		//buffArranger.setSkylineBoyut(selectedSkylineFields);
		
	}
	Schema sch;
	int diskErisimSayýsý,diskErisimSayýsý2;
//	String algoType;
//	int windowSize;
	BufferArranger buffArranger;
	InitOneTable init;
	public Test1(Schema sch, ArrayList<String> skyFld){
		this.sch = sch;
		this.selectedSkylineFields = skyFld;
	}
	public Test1(Schema sch){
		//System.out.println("þema tamam mý??");
		this.sch = sch;
		for(String sch1 : sch.fields())
			System.out.println("þema bilgisi in test1:" + sch1);
	}
	public void createSystem(int tupleSize, String distributionType,String tblName,String dbName, int buffSize,int click){
		init = new InitOneTable(tupleSize, distributionType, tblName, sch, dbName, buffSize, click);
//		if(init != null)
//			System.out.println("init mall");
		//init.dummy();
	}
	public void execCurrentSystem(String tblName, int bufferSize, String typeBnl,int clickCount){
		init = new InitOneTable(tblName);
		init.setSkylineFiels(getSelectedSkylineFields());
		init.dummy();
		diskErisimSayýsý = (FileMgr.getReadCount() + FileMgr.getWriteCount());
		System.out.println("önceki disk eriþim sayýsý:" + diskErisimSayýsý );
		buffArranger = new BufferArranger(typeBnl, selectedSkylineFields, bufferSize-3, tblName, clickCount);
		//buffArranger = new BufferArranger(typeBnl, selectedSkylineFields, buffSize - 3, tblName, clickCount);
		long algorithmStartTime = System.currentTimeMillis();
		buffArranger.inputToWindow();
//		buffArranger.window.beforeFirst();
//		while(buffArranger.window.next()){
//			System.out.println(" " + buffArranger.window.getInt("A"));
//			System.out.println(" " + buffArranger.window.getInt("B"));
//		}
		buffArranger.readFromTemp();
		
		diskErisimSayýsý2 = (FileMgr.getReadCount() + FileMgr.getWriteCount());
		System.out.println("sonraki disk eriþim sayýsý:" + diskErisimSayýsý2);
		System.out.println("hesap disk eriþim sayýsý:" + (diskErisimSayýsý2-diskErisimSayýsý));
		//buffArranger.getInput().close();
		long algorithmStopTime = System.currentTimeMillis();
		System.out.println("dosya iþlemlerinden sonra elde edilen skyline:");
		
		System.out.println(""+ buffArranger.getSkyline().getSkylinePoints().size()/init.getSkylineFields().size() + " skyline bulundu.");
		int indx = 0;
		for(Integer wind : buffArranger.getSkyline().getSkylinePoints()){
			//System.out.println(" " + wind.SIZE);
			if(indx < init.getSkylineFields().size()){
				System.out.print(" " + wind);
				indx++;
			}
			else{
				System.out.println();
				indx = 0;
				indx++;
				System.out.print(" " + wind);
				//System.out.println(" ");
			}
		}
		long processTime = algorithmStopTime - algorithmStartTime;
		
		System.out.println("algoritmanýn iþlem süresi:" + processTime/60000 + " dakika " + (processTime % 60) + " saniye");
	}
	
	public void execNewSystem(String typeBnl, int buffSize, String tblName, int tupleSize, String distributionType, String dbName, int clickCount) {
		// TODO Auto-generated method stub
//		String typeOfBnl = typeBnl;
//		int windSize = winSize;
//		String tableName = tblName;
		init = new InitOneTable(tblName);
		/*bnl,windsize,tableName burada atanacak.*/
		
		//init = new InitOneTable(tupleSize, distributionType, tblName, sch, dbName, buffSize,clickCount);
		if(init == null)
			System.out.println("init null");
		init.setSkylineFiels(getSelectedSkylineFields());
		
		init.dummy();
		
		
		//init.setSkylineFiels("A");
//		init.setSkylineFiels("B");
//		init.setSkylineFiels("C");
		//init.setSkylineFiels("F");
//		init.setSkylineFiels("J");
		
		//init.initData(dbName); // skyline isimli ornek bir VT.
											// Icerisinde INPUT(A int, B int)
											// tablosu var. Tabloda 999 tane
											// kayýt var.
		
	//	BufferArranger buffArranger = new BufferArranger("self organize bnl", init.getSkylineFields());
		//SkylineFinder  sky = new SkylineFinder();replace window bnl self organize bnl
		
		buffArranger = new BufferArranger(typeBnl, selectedSkylineFields, buffSize-3, tblName, clickCount);
		diskErisimSayýsý = (FileMgr.getReadCount() + FileMgr.getWriteCount());
		System.out.println("önceki disk eriþim sayýsý:" + diskErisimSayýsý );
		long algorithmStartTime = System.currentTimeMillis();
		buffArranger.inputToWindow();
//		buffArranger.window.beforeFirst();
//		while(buffArranger.window.next()){
//			System.out.println(" " + buffArranger.window.getInt("A"));
//			System.out.println(" " + buffArranger.window.getInt("B"));
//		}
		buffArranger.readFromTemp();
		long algorithmStopTime = System.currentTimeMillis();
		//buffArranger.getInput().close();
		diskErisimSayýsý2 = (FileMgr.getReadCount() + FileMgr.getWriteCount());
		System.out.println("sonraki disk eriþim sayýsý:" + diskErisimSayýsý2);
		System.out.println("hesap disk eriþim sayýsý:" + (diskErisimSayýsý2-diskErisimSayýsý));
		
		System.out.println("dosya iþlemlerinden sonra elde edilen skyline:");
		
		System.out.println(""+ buffArranger.getSkyline().getSkylinePoints().size()/init.getSkylineFields().size() + " skyline bulundu.");
		int indx = 0;
		for(Integer wind : buffArranger.getSkyline().getSkylinePoints()){
			//System.out.println(" " + wind.SIZE);
			if(indx < init.getSkylineFields().size()){
				System.out.print(" " + wind);
				indx++;
			}
			else{
				System.out.println();
				indx = 0;
				indx++;
				System.out.print(" " + wind);
				//System.out.println(" ");
			}
		}
		long processTime = algorithmStopTime - algorithmStartTime;
		
		System.out.println("algoritmanýn iþlem süresi:" + processTime/60000 + " dakika " + (processTime % 60) + " saniye");
		/*52-57 arasý gui'ye gerekli deðerleri göndermek için aksi halde her seferinde yeni bir input table üretmek zorunda kalýrýz.*/
//		int [][] skylines = new int[SkylineFinder.skylinePoints.size()/init.getSkylineFields().size()][init.getSkylineFields().size()];
//		for(int i = 0; i < (SkylineFinder.skylinePoints.size()/init.getSkylineFields().size()); i++){
//			for(int u = 0; u < init.getSkylineFields().size(); u++){
//				skylines[i][u] = SkylineFinder.skylinePoints.get(*i);
//				//skylines[i][1] = SkylineFinder.skylinePoints.get(2*i+1);
//			}
//			
//		}
		//System.out.println("skyline size in test1:" + skylines.length);
	//	GraphicTest gt = new GraphicTest(BufferArranger.allOfTuples, skylines);
		
		
	}
	

}
