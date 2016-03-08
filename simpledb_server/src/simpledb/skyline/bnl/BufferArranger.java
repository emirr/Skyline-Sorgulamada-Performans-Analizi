package simpledb.skyline.bnl;

import java.util.ArrayList;
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
	Transaction tx = new Transaction();
	MetadataMgr md = SimpleDB.mdMgr();

	/*
	 * input tablosunundaki kayýtlarý sýrayla okuyoruz. Burda 1 tampon(page)
	 * kullanýyor.
	 */

	TableInfo ti = md.getTableInfo("input", tx);
	RecordFile input = new RecordFile(ti, tx);

	/* window alaný oluþturulur */
	TableInfo tiWindow = new TableInfo("tempWindowFile", ti.schema());
	WindowUpdateScan window = new WindowUpdateScan(tiWindow, 0, 4, tx);

	/* output alaný oluþturulur. */
	TempTable temp = new TempTable(ti.schema(), tx);
	TableScan tempfile = (TableScan) temp.open();
	private ArrayList<String> skylineBoyut;
	int z = 0;
	int i = 0;// tempfile iterasyon sayýsý
	int l = 0;
	SkylineFinder skyline;
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
	BufferArranger(String type, ArrayList<String> skylineBoyutu){
		this.typeOfBnl = type;
		this.skylineBoyut = skylineBoyutu;
		skyline = new SkylineFinder(skylineBoyut);
	}
	

	public String getTypeOfBnl(){
		return typeOfBnl;
	}
	public ArrayList<String> getSkylineBoyut(){
		return skylineBoyut;
	}
	// inputdaki deðerler domine durumlarýna göre windowa,windowda yer yoksa
	// tempFile'a yerleþtirilir
	public void inputToWindow() {
		while (input.next()) {
			/*inputDominationMap bir inputa ait domine etme bilgisi taþýr.Bu nedenle
			 * her yeni input için sýfýrlanmasý gerekir.*/
			skyline.clearInputDominationMap();
			
			/*System.out.println("" + l + "." + " kayýt");*/
			
			// allOfTuples[z][0] = input.getInt("A");
			// allOfTuples[z][1] = input.getInt("B");
			
			/*for (String tempField : getSkylineBoyut()) {
				//System.out.println(tempField);
			//	window.setInt(tempField, input.getInt(tempField));
				
				System.out.print(" " + input.getInt(tempField));
				
			}*/
			/*System.out.println();*/
			
			// System.out.println("" + z + "." + " kayýt okundu");
			/*l++;*/
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
					input.next();
					// allOfTuples[z][0] = input.getInt("A");
					// allOfTuples[z][1] = input.getInt("B");
					
					/*System.out.println("" + l + "." + " kayýt");*/
					/*for (String tempField : getSkylineBoyut()) {*/
						//System.out.println(tempField);
					//	window.setInt(tempField, input.getInt(tempField));
						
						/*System.out.print(" " + input.getInt(tempField));*/
						
					/*}*/
					/*System.out.println();*/
					/*System.out.print("" + input.getInt("A") + " ");
					System.out.print("" + input.getInt("B") + " ");*/
					
					// System.out.println("" + z + "." + " kayýt okundu");
					/*l++;*/
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
						if (z > 0)
							replacedInwindow.add(window.getRid());
					} else {
						window.delete();
					}

				} else {
					if (skyline.compareDomination4Input() == -1) {
						if(typeOfBnl.equals("self organize bnl")){
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
						if (i > 0) 
							input.delete();
						if (z > 0)
							replacedInwindow.add(window.getRid());
						

					} else {
						// if()
						z++;
						putToTemp();
					}
				} else if (i > 0)
					input.delete();

			}
		}
	}

	// windowa sýðmayan ve olasý skyline noktalar tempfile'a aktarýldý.
	public void putToTemp() {
		if (z == 1)
			replacedInwindow = new HashSet<>();
		if (i == 0) {
			tempfile.insert();
			skyline.setTempFile(tempfile);
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

					TableInfo ti2 = new TableInfo("temp1", ti.schema());
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

			} else
				break;
		}

	}
}
