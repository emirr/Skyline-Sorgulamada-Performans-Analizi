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
	 * input tablosunundaki kay�tlar� s�rayla okuyoruz. Burda 1 tampon(page)
	 * kullan�yor.
	 */

	TableInfo ti = md.getTableInfo("input", tx);
	RecordFile input = new RecordFile(ti, tx);

	/* window alan� olu�turulur */
	TableInfo tiWindow = new TableInfo("tempWindowFile", ti.schema());
	WindowUpdateScan window = new WindowUpdateScan(tiWindow, 0, 0, tx);

	/* output alan� olu�turulur. */
	TempTable temp = new TempTable(ti.schema(), tx);
	TableScan tempfile = (TableScan) temp.open();

	SkylineFinder skyline = new SkylineFinder();
	int z = 0;
	static int[][] allOfTuples = new int[InitOneTable.numberOfTuples][2];

	// inputdaki de�erler domine durumlar�na g�re windowa,windowda yer yoksa
	// tempFile'a yerle�tirilir
	public void inputToWindow() {
		while (input.next()) {

			// System.out.println("" + z +"." + " kay�t");
			allOfTuples[z][0] = input.getInt("A");
			allOfTuples[z][1] = input.getInt("B");
			System.out.print("" + input.getInt("A") + " ");
			System.out.print("" + input.getInt("B") + " ");
			System.out.println("" + z + "." + " kay�t okundu");
			z++;
			int inputIsDominatedFlag = 0;
			skyline.setInput(input);
			// if (input.next()) {
			// inputdaki ilk blo�un ilk kayd� do�rudan windowa
			// yerle�tirilsin
			if (input.currentRid().equals(new RID(0, 0))) {
				window.insert();
				skyline.setWindow(window);
				skyline.putRecordToWindow();
				input.next();
				allOfTuples[z][0] = input.getInt("A");
				allOfTuples[z][1] = input.getInt("B");
				System.out.print("" + input.getInt("A") + " ");
				System.out.print("" + input.getInt("B") + " ");
				System.out.println("" + z + "." + " kay�t okundu");
				z++;
				skyline.setInput(input);
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
					} else {
						window.delete();
						skyline.removeTimeRecord();
						// skyline.setWindow(window);
					}

				} else {
					if (skyline.compareDomination4Input() == -1) {
						inputIsDominatedFlag++;// e�er windowun en sonundaki
												// de�er
						// input de�erini domine ederse i�ler kar���r.bu d��ken
						// buna �nlme al�r.
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
					} else {
						putToTemp();
					}

				}
			}
		}

		// readFromTemp(); input olarak temporaryFile'� oku.
		// }
	}

	// windowa s��mayan ve olas� skyline noktalar tempfile'a aktar�ld�.
	public void putToTemp() {
		tempfile.insert();
		skyline.setTempFile(tempfile);
		skyline.putRecordToTemp();
		// z++;
		// System.out.println("" + z +"." + " kay�t");

	}

	public void readFromTemp() {
		int i = 0;
		long startTimeOfIteration;

		/*
		 * bir alt sat�rdaki kodu eklemezsen tempFiel'�n ilk eleman� windowla
		 * kar��la�t�r�lamaz.
		 */

		while (!skyline.isEndOfTempIterarion()) {
			tempfile.beforeFirst();
			skyline.setTempFile(tempfile);
			startTimeOfIteration = System.currentTimeMillis();
			i++;
			System.out.println(" " + i + "." + " iteration" + " starts at:" + startTimeOfIteration);
			while (tempfile.next()) {
				skyline.setTempFile(tempfile);
				// metod d�zeltildi
				if (!skyline.isTempElementComparedBefore()) {
					window.beforeFirst();
					skyline.setWindow(window);
					while (window.next()) {
						skyline.setWindow(window);

						/*
						 * E�er tempfile de�eri windeki de�erden daha �nce
						 * yaz�lm��sa domine durumlar�n� kar��la�t�r aksi halde
						 * kar��la�t�rma
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
											 * window i�ideki gezintiyi bitir ve
											 * s�radaki inputa ge�
											 */
								}
							}

						}

					}
					/* buray� silebilirsin */
					if (skyline.anyDominationByTemp())
						skyline.removeTimeOfTempRecord();

					if (skyline.isTempElementNotComparable()) {
						/*
						 * e�er hala windowda yer varsa ve mevcut tempfile
						 * de�eri hala windowdaki de�erler ile domine etme
						 * durumunda de�ilse bu temp de�erini windowa ekle.
						 */
						if (window.insert()) {
							/*
							 * window alan�n� temp'e setler sonra da temp
							 * alan�n� windowa yazar.
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
			/* tempFile iterasyonu bitti skyline'� belirle */
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
