package simpledb.skyline.bnl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import static java.sql.Types.*;

import simpledb.multibuffer.WindowUpdateScan;
import simpledb.query.TableScan;
import simpledb.record.RID;
import simpledb.record.RecordFile;
import simpledb.record.Schema;

public class SkylineFinder {
	private WindowUpdateScan window;
	private TableScan tempFile;
	private RecordFile input, input2;
	private int diskerisimsayisi;
	private RID windRID, inpRID;
	private int id, blkNo;
	private Schema sch;
	private int h;
	HashMap<String, Object> inpList = new HashMap<>();

	public HashMap<String, Object> getInpList() {
		return inpList;
	}

	private HashMap<RID, Double> performansCarpani = new HashMap<>();// RID
																		// window
																		// de�erinin
																		// konumunun,Long
																		// ise
																		// �zelliklerin
																		// �arp�m�n�n
																		// tipidir.
	private ArrayList<String> skylineDimensions;

	public void setSkylineDimensions(ArrayList<String> skylineDimensions) {
		this.skylineDimensions = skylineDimensions;
	}

	// private int tempRecordSize;// bu silinmeli 4...
	// private static int k, l;
	private int upsize;
	private Map<RID, RID> inputDominationMap = new HashMap<>();
	private ArrayList<Object> skylinePoints;

	SkylineFinder(ArrayList<String> skylineDimension, Schema schName) {
		this.skylineDimensions = skylineDimension;
		this.skylinePoints = new ArrayList<>();
		this.sch = schName;
	}

	public ArrayList<Object> getSkylinePoints() {
		return skylinePoints;
	}

	public ArrayList<String> getSkylineDimensions() {
		return skylineDimensions;
	}

	/*
	 * input windowda kar��la�t�r�ld��� de�eri domine ediyorsa 1, input domine
	 * ediliyorsa -1, ikisi de domine edilemiyorsa 0 d�nd�r�l�r
	 */
	public int compareDomination4Input() {

		int k = 0;
		upsize = getSkylineDimensions().size();
		for (String tempField : getSkylineDimensions()) {
			//System.out.println("tempfield:" + tempField + "alan tipi:" + sch.type(tempField));
			//System.out.println("" + input.getInt(tempField));
			if (sch.type(tempField) == INTEGER){
				//System.out.println("input alan:" + tempField + " " + input.getInt(tempField));
				//System.out.println("window alan:" + tempField + " " + window.getInt(tempField));
				k += deepCompare(input.getInt(tempField), window.getInt(tempField));
				
			}
			if (sch.type(tempField) == DOUBLE)
				k += deepCompare(input.getDouble(tempField), window.getDouble(tempField));
		}
		if ((upsize != 0) && k == upsize)
			return 1;
		else {
			if ((upsize != 0) && k == -upsize)
				return -1;
			else
				return 0;
		}

	}

	public int deepCompare(Object x, Object y) {
		int m = -2;
		if (x instanceof Integer && y instanceof Integer) {
			if ((int) x < (int) y)
				m = 1;
			else {
				if ((int) x > (int) y)
					m = -1;
				else {
					upsize--;
					m = 0;
				}

			}
		}
		if (x instanceof Double && y instanceof Double) {
			if ((double) x < (double) y)
				m = 1;
			else {
				if ((double) x > (double) y)
					m = -1;
				else {
					upsize--;
					m = 0;
				}

			}
		}
		return m;
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
		h++;
		//System.out.println("inputdan windowa:" + h);
		for (String tempField : getSkylineDimensions()) {
			System.out.print(tempField + ":");
			if (sch.type(tempField) == INTEGER) {
				window.setInt(tempField, input.getInt(tempField));
				//System.out.print(" " + input.getInt(tempField) + "-");
			}
			// if (sch.type(tempField) == DOUBLE)
			else {
				window.setDouble(tempField, input.getDouble(tempField));
				//System.out.print(" " + input.getDouble(tempField) + " ");
			}
			// System.out.print(" " + input.getInt(tempField));

		}
		//System.out.println(" ");

	}

	void putRecordToTemp() {

		// System.out.println("kay�t tempFile'da");

		for (String tempField : getSkylineDimensions()) {
			// System.out.println(tempField);
			if (sch.type(tempField) == INTEGER)
				tempFile.setInt(tempField, input.getInt(tempField));
			if (sch.type(tempField) == DOUBLE)
				tempFile.setDouble(tempField, input.getDouble(tempField));
			// System.out.print(" " + tempFile.getInt(tempField));
		}

		// System.out.println(" ");

	}

	void putRecordToTemp(HashMap<String, Object> inpList) {
		for (Map.Entry<String, Object> entry : inpList.entrySet()) {

			// System.out.println(entry.getKey());
			if (sch.type(entry.getKey()) == INTEGER)
				tempFile.setInt(entry.getKey(), (int) inpList.get(entry.getKey()));
			if (sch.type(entry.getKey()) == DOUBLE)
				tempFile.setDouble(entry.getKey(), (double) inpList.get(entry.getKey()));
			// System.out.print(" " + tempFile.getInt(tempField));
		}
	}

	/*
	 * false d�nerse mevcut input de�eri windowdaki bir de�eri daha �nce domine
	 * etmemi� demektir.Aksi durumda ise input de�eri daha �nce bir window
	 * de�erini domine etmi� demektir.
	 */
	public boolean anyDominationByInput() {
		return (inputDominationMap.containsValue(input.currentRid()) ? true : false);
	}

	public WindowUpdateScan findSkyline(HashSet<RID> replaceInWindow) {

		window.beforeFirst();
		while (window.next()) {
			if (replaceInWindow == null || !replaceInWindow.contains(window.getRid())) {
				for (String tempField : getSkylineDimensions()) {
					if (sch.type(tempField) == INTEGER)
						skylinePoints.add(window.getInt(tempField));
					if (sch.type(tempField) == DOUBLE)
						skylinePoints.add(window.getDouble(tempField));

				}

				// skylinePoints.add(window.getInt("B"));
				window.delete();
			}

		}
		return window;
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

	/*
	 * inputDominationMap bir inputa ait domine etme bilgisi ta��r.Bu nedenle
	 * her yeni input i�in s�f�rlanmas� gerekir.
	 */
	public void clearInputDominationMap() {
		inputDominationMap.clear();
	}

	/* windowu selforganizer olarak yeniden d�zenler. */
	public HashSet<RID> selfOrganizer(HashSet<RID> replacedInWindow) {
		id = window.getRid().id();
		blkNo = window.getRid().blockNumber();
		// int tmpValues = 0;
		// int tmpValues1 = 0;
		Object tmpValues = 0;
		Object tmpValues1 = 0;

		// String tempField = new int[skylineDimensions];
		if (id != 0 || blkNo != 0) {
			System.out.println("window organize ediliyor");
			if (replacedInWindow != null) {
				if (replacedInWindow.contains(new RID(0, 0))) {
					if (!replacedInWindow.contains(new RID(blkNo, id))) {
						replacedInWindow.remove(new RID(0, 0));
						replacedInWindow.add(new RID(blkNo, id));
					}

				} else {
					if (replacedInWindow.contains(new RID(blkNo, id))) {
						replacedInWindow.remove(new RID(blkNo, id));
						replacedInWindow.add(new RID(0, 0));
					}
				}
			}
			window.beforeFirst();
			window.next();

			if (window.getRid().equals(new RID(0, 0))) {// windowun 0.blok
														// 0.slotu bo� de�il
														// demektir.
				for (String tempField : getSkylineDimensions()) {

					// tmpValues = window.getInt(tempField);//0.b 1.slot de�eri
					// window.moveToRid(new RID(0, 0));
					// tmpValues1 = window.getInt(tempField);//0-0 d�ri
					// window.setInt(tempField, tmpValues);
					//
					// window.moveToRid(new RID(blkNo, id));
					// window.setInt(tempField, tmpValues1);
					if (sch.type(tempField) == INTEGER)
						tmpValues1 = window.getInt(tempField);
					if (sch.type(tempField) == DOUBLE)
						tmpValues1 = window.getDouble(tempField);

					window.moveToRid(new RID(blkNo, id));
					if (sch.type(tempField) == INTEGER)
						tmpValues = window.getInt(tempField);
					if (sch.type(tempField) == DOUBLE)
						tmpValues = window.getDouble(tempField);
					if (sch.type(tempField) == INTEGER)
						window.setInt(tempField, (int) tmpValues1);// 0.b-1.s ye
																	// 0-0
																	// de�eri
																	// atand�.
					if (sch.type(tempField) == DOUBLE)
						window.setDouble(tempField, (double) tmpValues1);
					window.moveToRid(new RID(0, 0));
					if (sch.type(tempField) == INTEGER)
						window.setInt(tempField, (int) tmpValues);// 0.b-0.s a
																	// 0-1
																	// de�eri
					// atand�.
					if (sch.type(tempField) == DOUBLE)
						window.setDouble(tempField, (double) tmpValues);
				}
			} else {// 0-0 bo� demektir.

				// window.beforeFirst();
				// window.insert();
				for (String tempField : getSkylineDimensions()) {
					window.moveToRid(new RID(blkNo, id));
					if (sch.type(tempField) == INTEGER)
						tmpValues = window.getInt(tempField);
					if (sch.type(tempField) == DOUBLE)
						tmpValues = window.getDouble(tempField);
					window.moveToRid(new RID(0, 0));
					if (sch.type(tempField) == INTEGER)
						window.setInt(tempField, (int) tmpValues);
					if (sch.type(tempField) == DOUBLE)
						window.setDouble(tempField, (double) tmpValues);
					// window.moveToRid(new RID(blkNo, id));

				}
				window.insert();// 0-0 INUSE yap�ld�.
				window.moveToRid(new RID(blkNo, id));
				window.delete();// 0-1 EMPTY yap�ld�.
			}

		}
		return replacedInWindow;
	}

	public HashMap<RID, Double> getPerformansCarpani() {
		return performansCarpani;
	}

	// windowdaki her bir de�erin skyline �zelliklerinin �arp�m�,window
	// adresleri key olacak �ekilde tutulur.
	public void performansCarpanHesapla() {
		// long carpanDegeri = 1;
		double carpanDegeri = 1.0;
		for (String tempField : getSkylineDimensions()) {
			if (sch.type(tempField) == INTEGER)
				carpanDegeri = carpanDegeri * (double) window.getInt(tempField);
			if (sch.type(tempField) == DOUBLE)
				carpanDegeri = carpanDegeri * window.getDouble(tempField);
		}
		performansCarpani.put(window.getRid(), carpanDegeri);
	}

	public HashSet<RID> selectVictim(HashSet<RID> replacedInwindow, int iterasyonSay�s�) {
		// System.out.println("window replace ediliyor.");
		// long inputCarpanDegeri = 1;
		double inputCarpanDegeri = 1.0;
		inpList.clear();
		for (String tempField : getSkylineDimensions()) {
			if (sch.type(tempField) == INTEGER)
				inputCarpanDegeri = inputCarpanDegeri * (double) input.getInt(tempField);
			if (sch.type(tempField) == DOUBLE)
				inputCarpanDegeri = inputCarpanDegeri * input.getDouble(tempField);
		}

		window.beforeFirst();
		// long max = 0;// windowdaki max. �arpan de�eri
		double max = 0.0;
		window.next();
		max = performansCarpani.get(window.getRid());
		RID maxRID = window.getRid();
		while (window.next()) {
			if (max < performansCarpani.get(window.getRid())) {
				max = performansCarpani.get(window.getRid());
				maxRID = window.getRid();
			}
		}
		if (inputCarpanDegeri < max) {
			/* yer de�i�imi yap�lacak */
			window.moveToRid(maxRID);
			// RID currentInputRID = input.currentRid();
			// RID lastInputRID = null;
			// input.beforeFirst();
			// while(input.next()){
			// lastInputRID = input.currentRid();
			// }
			Object tempWindow = null;
			Object tempInput = null;
			// input2 = input;
			// inpList.clear();
			for (String tempField : getSkylineDimensions()) {
				if (sch.type(tempField) == INTEGER)
					tempWindow = window.getInt(tempField);
				if (sch.type(tempField) == DOUBLE)
					tempWindow = window.getDouble(tempField);
				// input.moveToRid(currentInputRID);
				if (sch.type(tempField) == INTEGER)
					tempInput = input.getInt(tempField);
				if (sch.type(tempField) == DOUBLE)
					tempInput = input.getDouble(tempField);

				if (sch.type(tempField) == INTEGER)
					window.setInt(tempField, (int) tempInput);
				if (sch.type(tempField) == DOUBLE)
					window.setDouble(tempField, (double) tempInput);
				// System.out.println("windowa gelen:" + tempInput);

				System.out.println("window replace ediliyor...");
				// input.moveToRid(lastInputRID);
				// input.insert();
				if (iterasyonSay�s� == 0) {
					inpList.put(tempField, tempWindow);
				} else {
					if (sch.type(tempField) == INTEGER)
						input.setInt(tempField, (int) tempWindow);
					if (sch.type(tempField) == DOUBLE)
						input.setDouble(tempField, (double) tempWindow);
					// System.out.println("ge�emedi");
				}

			}
			// input.close();
			if (replacedInwindow != null && !replacedInwindow.contains(maxRID)) {
				replacedInwindow.add(maxRID);
			}
		}
		return replacedInwindow;
	}

	public RecordFile getInput() {
		return input;
	}

	public RecordFile getInput2() {
		return input2;
	}
}
