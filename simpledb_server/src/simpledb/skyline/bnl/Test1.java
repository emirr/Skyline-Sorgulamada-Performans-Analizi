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
 * Bu testin amacý 8 adet tampon bölgesinin koordinasyonunu anlamaktir. Tamponumuz 3 kýsma ayrýldi.
 * 1 tampon: input dosyasýndan gelen kayýtlar
 * 5 tampon: window bölgemiz.
 * 2 tampon: tempfile'a yazana output buffer'dir. Tempfile'in geniþlemesi için auni anda 2 tampona ihtiyaç var.
 */

public class Test1 {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		InitOneTable init = new InitOneTable();
		//init.setSkylineFiels("A");
		init.setSkylineFiels("B");
		//init.setSkylineFiels("C");
		init.setSkylineFiels("F");
		init.setSkylineFiels("J");
		init.initData("skyline"); // skyline isimli ornek bir VT.
											// Icerisinde INPUT(A int, B int)
											// tablosu var. Tabloda 999 tane
											// kayýt var.
		
		BufferArranger buffArranger = new BufferArranger("self organize bnl", init.getSkylineFields());
		//SkylineFinder  sky = new SkylineFinder();
		
		buffArranger.inputToWindow();
//		buffArranger.window.beforeFirst();
//		while(buffArranger.window.next()){
//			System.out.println(" " + buffArranger.window.getInt("A"));
//			System.out.println(" " + buffArranger.window.getInt("B"));
//		}
		buffArranger.readFromTemp();
		System.out.println("dosya iþlemlerinden sonra elde edilen skyline:");
		System.out.println(""+ SkylineFinder.skylinePoints.size()/init.getSkylineFields().size() + " skyline bulundu.");
		int indx = 0;
		for(Integer wind : SkylineFinder.skylinePoints){
			//System.out.println(" " + wind.SIZE);
			//if(indx < init.getSkylineFields().size()){
				System.out.println(" " + wind);
				//indx++;
			//}
			//else{
				//indx = 0;
				//System.out.println(" ");
			//}
		}
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
