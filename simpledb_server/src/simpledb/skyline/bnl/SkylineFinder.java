package simpledb.skyline.bnl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import simpledb.multibuffer.WindowUpdateScan;
import simpledb.query.TableScan;
import simpledb.record.RID;
import simpledb.record.RecordFile;

public class SkylineFinder {
	private WindowUpdateScan window;
	private TableScan tempFile;
	private RecordFile input;
	private RID windRID, inpRID, tmpRID;
	private int id, blkNo;
	private static int k, l;
	// private Map<RecordFile, WindowUpdateScan> inputDominationMap = new
	// HashMap<>();
	// private Map<TableScan, WindowUpdateScan> tempDominationMap = new
	// HashMap<>();
	private Map<RID, RID> inputDominationMap = new HashMap<>();
	private Map<RID, RID> tempDominationMap = new HashMap<>();
	/*
	 * TempFile iterasyonu sirasýnda window tarafýndan domine edilen tempFile
	 * elemanlarýnýn kaydýný tutar.
	 */
	private ArrayList<RID> dominatedElementsOfTemp = new ArrayList<>();
	private ArrayList<RID> skylineElementsOfTemp = new ArrayList<>();
	private Map<RID, Long> timeOfRecord = new HashMap<>();
	private Map<RID, Long> timeOfTempRecord = new HashMap<>();
	static ArrayList<Integer> skylinePoints = new ArrayList<>();
	private int tempRecordSize;

	// SkylineFinder(WindowUpdateScan wind, RecordFile input) {
	// this.window = wind;
	// this.input = input;
	// }
	//
	// SkylineFinder(WindowUpdateScan wind, TableScan temp) {
	// this.window = wind;
	// this.tempFile = temp;
	// }
	SkylineFinder() {

	}
	// public boolean isThereDomination() {
	// return compare4Domination() > 0;
	// }

	/*
	 * input windowda karþýlaþtýrýldýðý deðeri domine ediyorsa 1, input domine
	 * ediliyorsa -1, ikisi de domine edilemiyorsa 0 döndürülür
	 */
	public int compareDomination4Input() {
		// if (timeOfRecord == null) {
		// return 1;// window'a ilk eleman eklenmesi için.
		// } else {
		if (input.getInt("A") < window.getInt("A")) {
			if (input.getInt("B") <= window.getInt("B")) {
				// inputDominationMap.put(window, input);
				return 1;// input elemaný windowdakini domine etti.
			} else
				return 0;// inputdaki ve windowdaki eleman birbirini domine
							// edemedi.
		} else {
			if (input.getInt("A") > window.getInt("A")) {
				if (input.getInt("B") >= window.getInt("B"))
					return -1;// inputdaki eleman domine edildi.
				else
					return 0;
			} else {
				if (input.getInt("B") > window.getInt("B"))
					return -1;
				else {
					if (input.getInt("B") < window.getInt("B")) {
						// inputDominationMap.put(window, input);
						return 1;
					}

					else
						return 0;
				}
			}
		}
	}

	public void setInputDominationMap() {

		id = window.getRid().id();
		blkNo = window.getRid().blockNumber();
		windRID = new RID(blkNo, id);

		id = input.currentRid().id();
		blkNo = input.currentRid().blockNumber();
		inpRID = new RID(blkNo, id);

		inputDominationMap.put(windRID, inpRID);
	}

	public void putRecordToWindow() {

		window.setInt("A", input.getInt("A"));
		// System.out.println(" " + window.getInt("A"));
		window.setInt("B", input.getInt("B"));
		// System.out.println(" " + window.getInt("B"));
		long timestamp = System.currentTimeMillis();

		System.out.println("inputdan windowa:" + k++);
		System.out.print(" " + input.getInt("A"));
		System.out.print(" " + input.getInt("B"));
		System.out.println(" " + timestamp);

		id = window.getRid().id();
		blkNo = window.getRid().blockNumber();
		windRID = new RID(blkNo, id);
		// for (RID rid : timeOfRecord.keySet()) {
		// if (rid.equals(windRID)){
		// timeOfRecord.remove(rid, timeOfRecord.get(rid));
		// break;
		// }
		// }
		// removeTimeRec()
		// if(!timeOfRecord.isEmpty())
		removeTimeRecord(windRID);
		timeOfRecord.put(windRID, timestamp);
	}

	void putTempToWindow() {
		window.setInt("A", tempFile.getInt("A"));
		window.setInt("B", tempFile.getInt("B"));
		long timestamp = System.currentTimeMillis();
		System.out.println("tempFile'dan windowa:" + l++);
		System.out.print(" " + tempFile.getInt("A"));
		System.out.print(" " + tempFile.getInt("B"));
		System.out.println(" " + timestamp);
		id = window.getRid().id();
		blkNo = window.getRid().blockNumber();
		windRID = new RID(blkNo, id);

		// for (RID rid : timeOfRecord.keySet()) {
		// if (rid.equals(windRID)) {
		// timeOfRecord.remove(rid, timeOfRecord.get(rid));
		// break;
		// }
		// }
		// removetimeRec
		removeTimeRecord(windRID);
		timeOfRecord.put(windRID, timestamp);
	}

	void putRecordToTemp() {
		tempFile.setInt("A", input.getInt("A"));
		tempFile.setInt("B", input.getInt("B"));
		long timestamp = System.currentTimeMillis();

		id = tempFile.getRid().id();
		blkNo = tempFile.getRid().blockNumber();
		tmpRID = new RID(blkNo, id);

		timeOfTempRecord.put(tmpRID, timestamp);
		tempRecordSize++;
		System.out.println("inputdan tempFile'a");
		System.out.print(" " + tempFile.getInt("A"));
		System.out.print(" " + tempFile.getInt("B"));
		System.out.print(" " + timestamp );
		System.out.println(" " + tempRecordSize + "." + "kayýt tempFile'da");
	}

	/*
	 * false dönerse mevcut input deðeri windowdaki bir deðeri daha önce domine
	 * etmemiþ demektir.Aksi durumda ise input deðeri daha önce bir window
	 * deðerini domine etmiþ demektir.
	 */
	public boolean anyDominationByInput() {
		return (inputDominationMap.containsValue(input.currentRid()) ? true : false);
	}

	public boolean anyDominationByTemp() {
		return (tempDominationMap.containsValue(tempFile.getRid()) ? true : false);
	}

	/*
	 * input'daki deðer windowdaki deðeri domine ediyorsa 1, input'daki deðer
	 * domine oluyorsa -1, ikisi de domine edilemiyorsa 0 döner.
	 */
	public int compareDomination4Temp() {
		if (tempFile.getInt("A") < window.getInt("A")) {
			if (tempFile.getInt("B") <= window.getInt("B")) {
				// setTempDominationMap();
				return 1;// tempFile elemaný windowdakini domine etti.
			} else
				return 0;// inputdaki ve windowdaki eleman birbirini domine
							// edemedi.
		} else {
			if (tempFile.getInt("A") > window.getInt("A")) {
				if (tempFile.getInt("B") >= window.getInt("B"))
					return -1;// tempFiledaki eleman domine edildi.
				else
					return 0;
			} else {
				if (tempFile.getInt("B") > window.getInt("B"))
					return -1;
				else {
					if (tempFile.getInt("B") < window.getInt("B")) {
						//setTempDominationMap();
						return 1;
					}

					else
						return 0;
				}
			}
		}

	}

	/*
	 * tempFile'daki deðer daha önce yazýlmýþsa 1, windowdaki sonra yazýlmýþsa 0
	 * döner.
	 */
	// ---1
	public int compareRecordTime() {
		RID key1 = null;
		RID key2 = null;
		id = tempFile.getRid().id();
		blkNo = tempFile.getRid().blockNumber();
		tmpRID = new RID(blkNo, id);

		id = window.getRid().id();
		blkNo = window.getRid().blockNumber();
		windRID = new RID(blkNo, id);

		for (RID rid : timeOfRecord.keySet()) {
			if (rid.equals(windRID)) {
				key1 = rid;
				break;
			}
		}

		for (RID rid : timeOfTempRecord.keySet()) {
			if (rid.equals(tmpRID)) {
				key2 = rid;
				break;
			}
		}
		

		if (timeOfRecord.get(key1) >= timeOfTempRecord.get(key2))
			return 1;
		else
			return 0;

	}

	public void markTempElementAsDominated() {
		id = tempFile.getRid().id();
		blkNo = tempFile.getRid().blockNumber();
		tmpRID = new RID(blkNo, id);

		dominatedElementsOfTemp.add(tmpRID);
	}

	public void markTempElementAsSkyline(RID tempRID) {
		skylineElementsOfTemp.add(tempRID);
	}

	// --3 contains metodu farklý objelerin deðerlerini RID equals metoduna göre
	// karþýlaþtýracaðýndan sorun yok.
	public boolean isTempElementComparedBefore() {
		RID key2 = null;
		id = tempFile.getRid().id();
		blkNo = tempFile.getRid().blockNumber();
		tmpRID = new RID(blkNo, id);

		for (RID rid : timeOfTempRecord.keySet()) {
			if (rid.equals(tmpRID)) {
				key2 = rid;
				break;
			}
		}
		if ((key2 == null) || (dominatedElementsOfTemp.contains(tempFile.getRid())
				|| skylineElementsOfTemp.contains(tempFile.getRid())))

			return true;

		else
			return false;
	}

	public boolean isEndOfTempIterarion() {
		if ((timeOfTempRecord.isEmpty() && timeOfRecord.isEmpty()))
			return true;
		else
			return false;
	}

	/*
	 * Temp elemaný window'daki deðerleri ne domine edebildi ne de domine edildi
	 * ise true döner.
	 */
	public boolean isTempElementNotComparable() {
		RID key = null;
		id = tempFile.getRid().id();
		blkNo = tempFile.getRid().blockNumber();
		tmpRID = new RID(blkNo, id);

		for (RID rid : timeOfTempRecord.keySet()) {
			if (rid.equals(tmpRID)) {
				key = rid;
				break;
			}
		}
		if (key != null) {
			if (!dominatedElementsOfTemp.contains(tempFile.getRid())
					&& !tempDominationMap.containsValue(tempFile.getRid()))
				return true;
			else
				return false;
		} else
			return false;
	}

	// --2
	public boolean findSkyline(long startOfTempIteration) {
		RID key = null;

		id = window.getRid().id();
		blkNo = window.getRid().blockNumber();
		windRID = new RID(blkNo, id);

		for (RID rid : timeOfRecord.keySet()) {
			if (rid.equals(windRID)) {
				key = rid;
				break;
			}
		}

		if (timeOfRecord.get(key) <= startOfTempIteration) {
			// window.moveToRid(windRID);
			skylinePoints.add(window.getInt("A"));
			// System.out.println("djdj");
			System.out.print(" " + window.getInt("A"));
			skylinePoints.add(window.getInt("B"));
			System.out.println(" " + window.getInt("B"));
			if (tempDominationMap.containsKey(key)){
				markTempElementAsSkyline(tempDominationMap.get(key));
				removeTimeOfTempRecord();
			}
			return true;

		}

		else
			return false;
	}

	public void setTempFile(TableScan temp) {
		this.tempFile = temp;
	}

	public void setWindow(WindowUpdateScan wind) {
		this.window = wind;
	}

	public void setInput(RecordFile inp) {
		this.input = inp;
	}

	public void setTempDominationMap() {
		id = window.getRid().id();
		blkNo = window.getRid().blockNumber();
		windRID = new RID(blkNo, id);

		id = tempFile.getRid().id();
		blkNo = tempFile.getRid().blockNumber();
		tmpRID = new RID(blkNo, id);

		tempDominationMap.put(windRID, tmpRID);
	}

	public void removeTimeRecord() {
		id = window.getRid().id();
		blkNo = window.getRid().blockNumber();
		removeTimeRecord(new RID(blkNo, id));
	}

	private void removeTimeRecord(RID windRID) {
		for (RID rid : timeOfRecord.keySet()) {
			if (rid.equals(windRID)) {
				timeOfRecord.remove(rid, timeOfRecord.get(rid));
				break;
			}
		}
	}

	public void removeTimeOfTempRecord() {
		id = tempFile.getRid().id();
		blkNo = tempFile.getRid().blockNumber();
		removeTimeOfTempRecord(new RID(blkNo, id));
	}

	private void removeTimeOfTempRecord(RID tmpRID) {
		for (RID rid : timeOfTempRecord.keySet()) {
			if (rid.equals(tmpRID)) {
				timeOfTempRecord.remove(rid, timeOfTempRecord.get(rid));
				break;
			}
		}
	}

}
