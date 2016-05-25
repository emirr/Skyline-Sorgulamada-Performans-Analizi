package simpledb.skyline.bnl;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.HashMap;
//import java.util.ArrayList;
import java.util.HashSet;

import simpledb.file.FileMgr;
import simpledb.materialize.TempTable;
import simpledb.metadata.MetadataMgr;
import simpledb.multibuffer.WindowUpdateScan;
import simpledb.query.TableScan;
import simpledb.record.RID;
import simpledb.record.RecordFile;
import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class BufferArranger {
	private String typeOfBnl;
	private String tempFileName;
	//private int windowSize;
	int clickcount;
	String dbName;
	Transaction tx = new Transaction();
	MetadataMgr md = SimpleDB.mdMgr();

	/*
	 * input tablosunundaki kayýtlarý sýrayla okuyoruz. Burda 1 tampon(page)
	 * kullanýyor.
	 */

	TableInfo ti;
	RecordFile input;

	/* window alaný oluþturulur */
	TableInfo tiWindow;
	WindowUpdateScan window;

	/* output alaný oluþturulur. */
	TempTable temp;
	TableScan tempfile;
	
	
	private ArrayList<String> skylineBoyut;
	public void setSkylineBoyut(ArrayList<String> skylineBoyut) {
		this.skylineBoyut = skylineBoyut;
		//skyline.setSkylineDimensions(skylineBoyut);
	}

	int z = 0;
	int i = 0;// tempfile iterasyon sayýsý
	int l = 0;
	SkylineFinder skyline;
	public SkylineFinder getSkyline() {
		return skyline;
	}

	// static int[][] allOfTuples = new int[InitOneTable.numberOfTuples][2];
	private HashSet<RID> replacedInwindow;// tempFile'a her ilk kayýt
											// giriþinden itibaren obje
											// oluþturulur.input iterasyonu
											// sýrasýnda domine edilme durumu
											// olursa dominenin gerçekleþtiði
											// RID
											// listeye eklenir.input iterasyonu
											// sonunda bu listede olmayan ve
											// dolu olan tüm window RID'ler
											// skyline'dir.
	public BufferArranger(String dbName, String type, ArrayList<String> skylineBoyutu, int windSize, String tableName, int clickCount){
		this.typeOfBnl = type;
		//this.windowSize = windSize;
		this.skylineBoyut = skylineBoyutu;
//		skyline = new SkylineFinder(skylineBoyut);
		//tx = new Transaction();
		ti = md.getTableInfo(tableName, tx);
		skyline = new SkylineFinder(skylineBoyut,ti.schema());
		input = new RecordFile(ti, tx);
		String windTblName;
//		if(clickCount == 0)
//			windTblName = "tempWindowFile";
//		else
		windTblName = "tempWindowFile" + (clickCount+1);
		tiWindow = new TableInfo(windTblName, ti.schema());
		window = new WindowUpdateScan(tiWindow, 0, windSize-1, tx);
		temp = new TempTable(ti.schema(), tx);
		tempFileName = temp.getTableInfo().fileName();
		tempfile = (TableScan) temp.open();
		this.clickcount = clickCount;
		this.dbName = dbName;
	}
	

	


	public String getTypeOfBnl(){
		return typeOfBnl;
	}
	public ArrayList<String> getSkylineBoyut(){
		return skylineBoyut;
	}
	// inputdaki deðerler domine durumlarýna göre windowa,windowda yer yoksa
	// tempFile'a yerleþtirilir
	int diskerisim1 = (FileMgr.getReadCount() + FileMgr.getWriteCount());
	public void inputToWindow() {
		while (input.next()) {
			/*inputDominationMap bir inputa ait domine etme bilgisi taþýr.Bu nedenle
			 * her yeni input için sýfýrlanmasý gerekir.*/
			skyline.clearInputDominationMap();
			
			//System.out.println("" + l + "." + " kayýt");
			
			// allOfTuples[z][0] = input.getInt("A");
			// allOfTuples[z][1] = input.getInt("B");
			
//			for (String tempField : getSkylineBoyut()) {
//				//System.out.println(tempField);
//			//	window.setInt(tempField, input.getInt(tempField));
//				
//				System.out.print(" " + input.getInt(tempField));
//				
//			}
//			System.out.println();
			
			// System.out.println("" + z + "." + " kayýt okundu");
			//l++;
			//long inputCarpanDegeri = 1;
			int inputIsDominatedFlag = 0;
			skyline.setInput(input);
			// if (input.next()) {
			// inputdaki ilk bloðun ilk kaydý doðrudan windowa
			// yerleþtirilsin
			if (i == 0) {
				if (input.currentRid().equals(new RID(0, 0))) {
					window.insert();
					skyline.setWindow(window);
					skyline.putRecordToWindow();
					if(typeOfBnl.equals("rplc win. bnl")){
						skyline.performansCarpanHesapla();
					}
					input.next();
					
					
					// allOfTuples[z][0] = input.getInt("A");
					// allOfTuples[z][1] = input.getInt("B");
					
//				System.out.println("" + l + "." + " kayýt");
//					for (String tempField : getSkylineBoyut()) {
//						//System.out.println(tempField);
//					//	window.setInt(tempField, input.getInt(tempField));
//						
//						System.out.print(" " + input.getInt(tempField));
//						
//					}
//					System.out.println();
//					
//					l++;
					skyline.setInput(input);
				}
			}
			window.beforeFirst();
			while (window.next()) {
				skyline.setWindow(window);
				// alýnan window ve input deðerleri karþýlaþtýrýlýr.
				// input deðeri domine etmiþse windowdaki deðerin yerine
				// inputu yaz.
				if (skyline.compareDomination4Input() == 1) {
					if (!skyline.anyDominationByInput()) {
						skyline.setInputDominationMap();
						skyline.putRecordToWindow();
						if(typeOfBnl.equals("rplc win. bnl")){
							skyline.performansCarpanHesapla();
						}
						if (z > 0)
							replacedInwindow.add(window.getRid());
					} else {
						window.delete();
					}

				} else {
					if (skyline.compareDomination4Input() == -1) {
						if(typeOfBnl.equals("self org. bnl")){
							replacedInwindow = skyline.selfOrganizer(replacedInwindow);
						}
						inputIsDominatedFlag++;// eðer windowun en sonundaki
												// deðer
						// input deðerini domine ederse iþler karýþýr.bu dðþken
						// buna önlme alýr.
						if (i > 0)
							input.delete();// domine edilen tempfile deðeri
											// silindi.

						break;// window içideki gezintiyi
						// bitir ve
						// sýradaki inputa geç.
					}
				}

			}
			if (!window.next() && inputIsDominatedFlag == 0) {
				if (!skyline.anyDominationByInput()) {// eðer
					// döngüden
					// windowda
					// eleman
					// kalmadýðý
					// için
					// çýkmýþsak
					// ve yukarýdaki þartlar saðlanýyorsa bunun anlamý input
					// deðeri
					// ve windowdaki deðerler birbirini domine edemedi.
					if (window.insert()) {// eðer windowda yer kalmýþsa bu input
											// deðerini windowa ekle.Kalmamýþsa
											// tempe
						skyline.setWindow(window);
						// skyline.setInputDominationMap();
						skyline.putRecordToWindow();
						if(typeOfBnl.equals("rplc win. bnl")){
							skyline.performansCarpanHesapla();
						}
						if (i > 0) 
							input.delete();
						if (z > 0)
							replacedInwindow.add(window.getRid());
						

					} else {
						// if()
						
						if(typeOfBnl.equals("rplc win. bnl")){
							//if(z > 0)
								replacedInwindow = skyline.selectVictim(replacedInwindow, i);
								input = skyline.getInput();
						}
						z++;
						
//						if(i == 0){
//							
//							putToTemp(skyline.getInpList());
//						}
						//else
							putToTemp();
					}
				} else if (i > 0)
					input.delete();

			}
		}
	}
	//int diskerisim2 = (FileMgr.getReadCount() + FileMgr.getWriteCount());
	
	/*
	 * domine edilmeyen/etmeyen input deðerinin skyline özelliklerinin çarpýmý
	 * windowdakilerle karþýlaþtýrýlýr.
	 */
	
	// windowa sýðmayan ve olasý skyline noktalar tempfile'a aktarýldý.
	public void putToTemp() {
		if (z == 1)
			replacedInwindow = new HashSet<>();
		if (i == 0) {
			tempfile.insert();
			skyline.setTempFile(tempfile);
			if(typeOfBnl.equals("rplc win. bnl") && !skyline.getInpList().isEmpty())
				skyline.putRecordToTemp(skyline.getInpList());
			else
				skyline.putRecordToTemp();
		}
		// z++;
		// System.out.println("" + z +"." + " kayýt");

	}

	public void readFromTemp() {
		while (true) {
			tempfile.beforeFirst();
			skyline.setWindow(window);
			window = skyline.findSkyline(replacedInwindow);// windowun skyline
															// içermeyen hali
			if (replacedInwindow != null)
				replacedInwindow.clear();// skyline elemanlar windowda olmadýðý
											// için diðerlerinin
			// sonradan eklenmiþ olduklarý bilgisini tutmaya gerek yok.
			if (tempfile.next()) {
				input.close();// input file'ýn pinlediði tampon havuzunun
								// sýfýrýncý
								// bloðu üzerindeki pin kaldýrýldý.
				if (i == 0) {
					System.out.println("TempFiel adý:" + tempFileName);
					String sub = null;
					if(tempFileName.substring(5, 6).equals("."))
						sub = tempFileName.substring(0, 5);
					
					else
						sub = tempFileName.substring(0, 6);
					
					TableInfo ti2 = new TableInfo(sub, ti.schema());
					input = new RecordFile(ti2, tx);// sýfýrýncý blok yeniden
													// pinlendi.Bu defa input,
													// input.tbl yerine
													// tempfile dosyasý için
													// okuma
													// yapar.
				}

				
				skyline.setInput(input);
				skyline.setWindow(window);

				i++;
				z = 0;
				input.beforeFirst();
				inputToWindow();

			} else{
//				input.close();
//				tempfile.close();
				tx.commit();
				break;
			}
				
		}

	}
	
	public long extraStrorageInfo(){
		long length = 0;
		String dirName = "C:\\Users\\oblmv2" + "\\" + dbName;
		//System.out.println(dirName);
		File dir = new File(dirName);
		FilenameFilter filter = new FilenameFilter() {
			public boolean accept(File dir, String name) {

				return name.startsWith("temp") ;

			}
		};
		File[] file = dir.listFiles(filter);
		int clc = clickcount+1;
		//System.out.println("clc:"+clc);
		if (file == null) {
			System.out.println("Either dir does not exist or is not a directory");
		} else {
			for (int i = 0; i < file.length; i++) {
				String filename = file[i].getName();
				//System.out.println(filename);
				if(clickcount<9){
					//System.out.println("clickcoun:"+clickcount);
					//System.out.println(""+filename.substring(4,5));
					if(filename.substring(4,5).equals(""+clc)){
						//System.out.println("temp file için ayrýlan1 alan:"+ file[i].length()/1024+"KB");
						length += file[i].length();

					}
					else if(filename.startsWith("tempWindow") && filename.substring(14,15).equals(""+clc)){
						System.out.println("tempWindow file için ayrýlan1 alan:"+ file[i].length()/1024+"KB");
						//length += file[i].length();
					}
				}
				else{
					if(filename.substring(4,6).equals(""+clc)){
						//System.out.println("temp file için ayrýlan1 alan:"+ file[i].length()/1024+"KB");
						length += file[i].length();

					}
					else if(filename.startsWith("tempWindow") && filename.substring(14,16).equals(""+clc)){
						//System.out.println("tempWindow file için ayrýlan1 alan:"+ file[i].length()/1024+"KB");
						length += file[i].length();
					}
				}
				
			}
		}
		return length;
	}

	public RecordFile getInput() {
		return input;
	}
}
