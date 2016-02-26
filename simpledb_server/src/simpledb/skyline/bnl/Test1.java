package simpledb.skyline.bnl;

import java.util.ArrayList;
import java.util.Iterator;
import simpledb.materialize.TempTable;
import simpledb.metadata.MetadataMgr;
import simpledb.multibuffer.WindowUpdateScan;
import simpledb.query.TableScan;
import simpledb.record.RID;
import simpledb.record.RecordFile;
import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/*
 * Bu testin amac� 8 adet tampon b�lgesinin koordinasyonunu anlamaktir. Tamponumuz 3 k�sma ayr�ldi.
 * 1 tampon: input dosyas�ndan gelen kay�tlar
 * 5 tampon: window b�lgemiz.
 * 2 tampon: tempfile'a yazana output buffer'dir. Tempfile'in geni�lemesi i�in auni anda 2 tampona ihtiya� var.
 */

public class Test1 {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		InitOneTable.initData("skyline"); // skyline isimli ornek bir VT.
											// Icerisinde INPUT(A int, B int)
											// tablosu var. Tabloda 999 tane
											// kay�t var.
		
		BufferArranger buffArranger = new BufferArranger();
		SkylineFinder  sky = new SkylineFinder();
		
		buffArranger.inputToWindow();
//		buffArranger.window.beforeFirst();
//		while(buffArranger.window.next()){
//			System.out.println(" " + buffArranger.window.getInt("A"));
//			System.out.println(" " + buffArranger.window.getInt("B"));
//		}
		buffArranger.readFromTemp();
		System.out.println("dosya i�lemlerinden sonra elde edilen skyline:");
		System.out.println(""+ SkylineFinder.skylinePoints.size()/2 + " skyline bulundu.");
		for(Integer wind : SkylineFinder.skylinePoints){
			//System.out.println(" " + wind.SIZE);
			System.out.println(" " + wind);
		}
		/*52-57 aras� gui'ye gerekli de�erleri g�ndermek i�in aksi halde her seferinde yeni bir input table �retmek zorunda kal�r�z.*/
		int [][] skylines = new int[SkylineFinder.skylinePoints.size()/2][2];
		for(int i = 0; i < (SkylineFinder.skylinePoints.size()/2); i++){
			skylines[i][0] = SkylineFinder.skylinePoints.get(2*i);
			skylines[i][1] = SkylineFinder.skylinePoints.get(2*i+1);
		}
		//System.out.println("skyline size in test1:" + skylines.length);
		GraphicTest gt = new GraphicTest(BufferArranger.allOfTuples, skylines);
		
		
	}

}
