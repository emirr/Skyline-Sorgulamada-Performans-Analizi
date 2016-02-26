package simpledb.skyline.bnl;

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
	WindowUpdateScan window = new WindowUpdateScan(tiWindow, 0, 0, tx);

	/* output alaný oluþturulur. */
	TempTable temp = new TempTable(ti.schema(), tx);
	TableScan tempfile = (TableScan) temp.open();

	SkylineFinder skyline = new SkylineFinder();
	int z = 0;
	static int[][] allOfTuples = new int[InitOneTable.numberOfTuples][2];

	// inputdaki deðerler domine durumlarýna göre windowa,windowda yer yoksa
	// tempFile'a yerleþtirilir
	public void inputToWindow() {
		while (input.next()) {

			// System.out.println("" + z +"." + " kayýt");
			allOfTuples[z][0] = input.getInt("A");
			allOfTuples[z][1] = input.getInt("B");
			System.out.print("" + input.getInt("A") + " ");
			System.out.print("" + input.getInt("B") + " ");
			System.out.println("" + z + "." + " kayýt okundu");
			z++;
			int inputIsDominatedFlag = 0;
			skyline.setInput(input);
			// if (input.next()) {
			// inputdaki ilk bloðun ilk kaydý doðrudan windowa
			// yerleþtirilsin
			if (input.currentRid().equals(new RID(0, 0))) {
				window.insert();
				skyline.setWindow(window);
				skyline.putRecordToWindow();
				input.next();
				allOfTuples[z][0] = input.getInt("A");
				allOfTuples[z][1] = input.getInt("B");
				System.out.print("" + input.getInt("A") + " ");
				System.out.print("" + input.getInt("B") + " ");
				System.out.println("" + z + "." + " kayýt okundu");
				z++;
				skyline.setInput(input);
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
					} else {
						window.delete();
						skyline.removeTimeRecord();
						// skyline.setWindow(window);
					}

				} else {
					if (skyline.compareDomination4Input() == -1) {
						inputIsDominatedFlag++;// eðer windowun en sonundaki
												// deðer
						// input deðerini domine ederse iþler karýþýr.bu dðþken
						// buna önlme alýr.
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
					} else {
						putToTemp();
					}

				}
			}
		}

		// readFromTemp(); input olarak temporaryFile'ý oku.
		// }
	}

	// windowa sýðmayan ve olasý skyline noktalar tempfile'a aktarýldý.
	public void putToTemp() {
		tempfile.insert();
		skyline.setTempFile(tempfile);
		skyline.putRecordToTemp();
		// z++;
		// System.out.println("" + z +"." + " kayýt");

	}

	public void readFromTemp() {
		int i = 0;
		long startTimeOfIteration;

		/*
		 * bir alt satýrdaki kodu eklemezsen tempFiel'ýn ilk elemaný windowla
		 * karþýlaþtýrýlamaz.
		 */

		while (!skyline.isEndOfTempIterarion()) {
			tempfile.beforeFirst();
			skyline.setTempFile(tempfile);
			startTimeOfIteration = System.currentTimeMillis();
			i++;
			System.out.println(" " + i + "." + " iteration" + " starts at:" + startTimeOfIteration);
			while (tempfile.next()) {
				skyline.setTempFile(tempfile);
				// metod düzeltildi
				if (!skyline.isTempElementComparedBefore()) {
					window.beforeFirst();
					skyline.setWindow(window);
					while (window.next()) {
						skyline.setWindow(window);

						/*
						 * Eðer tempfile deðeri windeki deðerden daha önce
						 * yazýlmýþsa domine durumlarýný karþýlaþtýr aksi halde
						 * karþýlaþtýrma
						 */
						if (skyline.compareRecordTime() == 1) {
							if (skyline.compareDomination4Temp() == 1) {
								if (!skyline.anyDominationByTemp()) {
									skyline.setTempDominationMap();
									skyline.putTempToWindow();
									// skyline.removeTimeOfTempRecord();

								} else {
									window.delete();
									skyline.removeTimeRecord();

									// skyline.setWindow(window);
								}
							} else {
								if (skyline.compareDomination4Temp() == -1) {
									skyline.markTempElementAsDominated();
									skyline.removeTimeOfTempRecord();
									break;/*
											 * window içideki gezintiyi bitir ve
											 * sýradaki inputa geç
											 */
								}
							}

						}

					}
					/* burayý silebilirsin */
					if (skyline.anyDominationByTemp())
						skyline.removeTimeOfTempRecord();

					if (skyline.isTempElementNotComparable()) {
						/*
						 * eðer hala windowda yer varsa ve mevcut tempfile
						 * deðeri hala windowdaki deðerler ile domine etme
						 * durumunda deðilse bu temp deðerini windowa ekle.
						 */
						if (window.insert()) {
							/*
							 * window alanýný temp'e setler sonra da temp
							 * alanýný windowa yazar.
							 */
							skyline.setWindow(window);
							// skyline.setTempDominationMap();
							skyline.putTempToWindow();
							// --mod: removeTimeOfTemp
							skyline.removeTimeOfTempRecord();
						}

					}

				}
			}
			/* tempFile iterasyonu bitti skyline'ý belirle */
			// else {
			window.beforeFirst();
			// skyline.setWindow(window);
			while (window.next()) {
				skyline.setWindow(window);
				if (skyline.findSkyline(startTimeOfIteration)) {
					window.delete();
					skyline.removeTimeRecord();
				}
			}
			// skyline.setWindow(window);
			// if (!skyline.isEndOfTempIterarion()) {
			// tempfile.beforeFirst();
			// skyline.setTempFile(tempfile);
			// startTimeOfIteration = System.currentTimeMillis();
			// } else
			// break;
			//
			// }
		}
	}
}
