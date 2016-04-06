package simpledb.skyline.bnl;

import java.util.ArrayList;
import java.util.HashMap;
//import java.util.ArrayList;
import java.util.HashSet;

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
	
	Transaction tx = new Transaction();
	MetadataMgr md = SimpleDB.mdMgr();

	/*
	 * input tablosunundaki kay�tlar� s�rayla okuyoruz. Burda 1 tampon(page)
	 * kullan�yor.
	 */

	TableInfo ti;
	RecordFile input;

	/* window alan� olu�turulur */
	TableInfo tiWindow;
	WindowUpdateScan window;

	/* output alan� olu�turulur. */
	TempTable temp;
	TableScan tempfile;
	
	
	private ArrayList<String> skylineBoyut;
	public void setSkylineBoyut(ArrayList<String> skylineBoyut) {
		this.skylineBoyut = skylineBoyut;
		//skyline.setSkylineDimensions(skylineBoyut);
	}

	int z = 0;
	int i = 0;// tempfile iterasyon say�s�
	int l = 0;
	SkylineFinder skyline;
	public SkylineFinder getSkyline() {
		return skyline;
	}

	// static int[][] allOfTuples = new int[InitOneTable.numberOfTuples][2];
	private HashSet<RID> replacedInwindow;// tempFile'a her ilk kay�t
											// giri�inden itibaren obje
											// olu�turulur.input iterasyonu
											// s�ras�nda domine edilme durumu
											// olursa dominenin ger�ekle�ti�i
											// RID
											// listeye eklenir.input iterasyonu
											// sonunda bu listede olmayan ve
											// dolu olan t�m window RID'ler
											// skyline'dir.
	BufferArranger(String type, ArrayList<String> skylineBoyutu, int windSize, String tableName, int clickCount){
		this.typeOfBnl = type;
		//this.windowSize = windSize;
		this.skylineBoyut = skylineBoyutu;
		skyline = new SkylineFinder(skylineBoyut);
		//tx = new Transaction();
		ti = md.getTableInfo(tableName, tx);
		input = new RecordFile(ti, tx);
		String windTblName;
		if(clickCount == 0)
			windTblName = "tempWindowFile";
		else
			windTblName = "tempWindowFile" + clickCount;
		tiWindow = new TableInfo(windTblName, ti.schema());
		window = new WindowUpdateScan(tiWindow, 0, windSize-1, tx);
		temp = new TempTable(ti.schema(), tx);
		tempFileName = temp.getTableInfo().fileName();
		tempfile = (TableScan) temp.open();
	}
	

	


	public String getTypeOfBnl(){
		return typeOfBnl;
	}
	public ArrayList<String> getSkylineBoyut(){
		return skylineBoyut;
	}
	// inputdaki de�erler domine durumlar�na g�re windowa,windowda yer yoksa
	// tempFile'a yerle�tirilir
	public void inputToWindow() {
		while (input.next()) {
			/*inputDominationMap bir inputa ait domine etme bilgisi ta��r.Bu nedenle
			 * her yeni input i�in s�f�rlanmas� gerekir.*/
			skyline.clearInputDominationMap();
			
			//System.out.println("" + l + "." + " kay�t");
			
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
			
			// System.out.println("" + z + "." + " kay�t okundu");
			//l++;
			//long inputCarpanDegeri = 1;
			int inputIsDominatedFlag = 0;
			skyline.setInput(input);
			// if (input.next()) {
			// inputdaki ilk blo�un ilk kayd� do�rudan windowa
			// yerle�tirilsin
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
					
//				System.out.println("" + l + "." + " kay�t");
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
				// al�nan window ve input de�erleri kar��la�t�r�l�r.
				// input de�eri domine etmi�se windowdaki de�erin yerine
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
						inputIsDominatedFlag++;// e�er windowun en sonundaki
												// de�er
						// input de�erini domine ederse i�ler kar���r.bu d��ken
						// buna �nlme al�r.
						if (i > 0)
							input.delete();// domine edilen tempfile de�eri
											// silindi.

						break;// window i�ideki gezintiyi
						// bitir ve
						// s�radaki inputa ge�.
					}
				}

			}
			if (!window.next() && inputIsDominatedFlag == 0) {
				if (!skyline.anyDominationByInput()) {// e�er
					// d�ng�den
					// windowda
					// eleman
					// kalmad���
					// i�in
					// ��km��sak
					// ve yukar�daki �artlar sa�lan�yorsa bunun anlam� input
					// de�eri
					// ve windowdaki de�erler birbirini domine edemedi.
					if (window.insert()) {// e�er windowda yer kalm��sa bu input
											// de�erini windowa ekle.Kalmam��sa
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
	/*
	 * domine edilmeyen/etmeyen input de�erinin skyline �zelliklerinin �arp�m�
	 * windowdakilerle kar��la�t�r�l�r.
	 */
	
	// windowa s��mayan ve olas� skyline noktalar tempfile'a aktar�ld�.
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
		// System.out.println("" + z +"." + " kay�t");

	}

	public void readFromTemp() {
		while (true) {
			tempfile.beforeFirst();
			skyline.setWindow(window);
			window = skyline.findSkyline(replacedInwindow);// windowun skyline
															// i�ermeyen hali
			if (replacedInwindow != null)
				replacedInwindow.clear();// skyline elemanlar windowda olmad���
											// i�in di�erlerinin
			// sonradan eklenmi� olduklar� bilgisini tutmaya gerek yok.
			if (tempfile.next()) {
				input.close();// input file'�n pinledi�i tampon havuzunun
								// s�f�r�nc�
								// blo�u �zerindeki pin kald�r�ld�.
				if (i == 0) {
					System.out.println("TempFiel ad�:" + tempFileName);
					String sub = null;
					if(tempFileName.substring(5, 6).equals("."))
						sub = tempFileName.substring(0, 5);
					
					else
						sub = tempFileName.substring(0, 6);
					
					TableInfo ti2 = new TableInfo(sub, ti.schema());
					input = new RecordFile(ti2, tx);// s�f�r�nc� blok yeniden
													// pinlendi.Bu defa input,
													// input.tbl yerine
													// tempfile dosyas� i�in
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





	public RecordFile getInput() {
		return input;
	}
}
