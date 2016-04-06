package simpledb.skyline.bnl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import simpledb.multibuffer.WindowUpdateScan;
import simpledb.query.TableScan;
import simpledb.record.RID;
import simpledb.record.RecordFile;

public class SkylineFinder {
	private WindowUpdateScan window;
	private TableScan tempFile;
	private RecordFile input,input2;
	private int diskerisimsayisi;
	private RID windRID, inpRID;
	private int id, blkNo;
	HashMap<String,Integer> inpList = new HashMap<>();
	public HashMap<String, Integer> getInpList() {
		return inpList;
	}

	private HashMap<RID, Long> performansCarpani = new HashMap<>();// RID window
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
	private ArrayList<Integer> skylinePoints ;

	SkylineFinder(ArrayList<String> skylineDimension) {
		this.skylineDimensions = skylineDimension;
		this.skylinePoints = new ArrayList<>();
	}

	public ArrayList<Integer> getSkylinePoints() {
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
			k += deepCompare(input.getInt(tempField), window.getInt(tempField));
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
		//System.out.println("inputdan windowa:" ); 
		for (String tempField : getSkylineDimensions()) {
			// System.out.println(tempField);
			window.setInt(tempField, input.getInt(tempField));

			//System.out.print(" " + input.getInt(tempField)); 

		}
		//System.out.println(" "); 
		

	}

	void putRecordToTemp() {
		
		
		// System.out.println("kay�t tempFile'da");
		 
		for (String tempField : getSkylineDimensions()) {
			// System.out.println(tempField);

			tempFile.setInt(tempField, input.getInt(tempField));
			//System.out.print(" " + tempFile.getInt(tempField)); 
		}

		//System.out.println(" "); 
		 
		
	}
	void putRecordToTemp(HashMap<String,Integer> inpList){
		for (Map.Entry<String, Integer> entry : inpList.entrySet()) {
		
		
			 //System.out.println(entry.getKey());

			tempFile.setInt(entry.getKey(), inpList.get(entry.getKey()));
			//System.out.print(" " + tempFile.getInt(tempField)); 
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
					skylinePoints.add(window.getInt(tempField));
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
		int tmpValues = 0;
		int tmpValues1 = 0;

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
					tmpValues1 = window.getInt(tempField);
					window.moveToRid(new RID(blkNo, id));
					tmpValues = window.getInt(tempField);
					window.setInt(tempField, tmpValues1);// 0.b-1.s ye 0-0
															// de�eri atand�.
					window.moveToRid(new RID(0, 0));
					window.setInt(tempField, tmpValues);// 0.b-0.s a 0-1 de�eri
														// atand�.
				}
			} else {// 0-0 bo� demektir.

				// window.beforeFirst();
				// window.insert();
				for (String tempField : getSkylineDimensions()) {
					window.moveToRid(new RID(blkNo, id));
					tmpValues = window.getInt(tempField);
					window.moveToRid(new RID(0, 0));
					window.setInt(tempField, tmpValues);
					// window.moveToRid(new RID(blkNo, id));

				}
				window.insert();// 0-0 INUSE yap�ld�.
				window.moveToRid(new RID(blkNo, id));
				window.delete();// 0-1 EMPTY yap�ld�.
			}

		}
		return replacedInWindow;
	}

	public HashMap<RID, Long> getPerformansCarpani(){
		return performansCarpani;
	}

	// windowdaki her bir de�erin skyline �zelliklerinin �arp�m�,window
	// adresleri key olacak �ekilde tutulur.
	public void performansCarpanHesapla() {
		long carpanDegeri = 1;
		for (String tempField : getSkylineDimensions()) {
			carpanDegeri = carpanDegeri * window.getInt(tempField);
		}
		performansCarpani.put(window.getRid(), carpanDegeri);
	}
	public HashSet<RID> selectVictim(HashSet<RID> replacedInwindow, int iterasyonSay�s�) {
		//System.out.println("window replace ediliyor.");
		long inputCarpanDegeri = 1;
		inpList.clear();
		for (String tempField : getSkylineDimensions()) {
			inputCarpanDegeri = inputCarpanDegeri * input.getInt(tempField);
		}

		window.beforeFirst();
		long max = 0;// windowdaki max. �arpan de�eri
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
//			RID currentInputRID = input.currentRid();
//			RID lastInputRID = null;
//			input.beforeFirst();
//			while(input.next()){
//				lastInputRID = input.currentRid();
//			}
			int tempWindow = 0;
			int tempInput = 0;
			//input2 = input;
//			inpList.clear();
			for (String tempField : getSkylineDimensions()) {
				tempWindow = window.getInt(tempField);
				//input.moveToRid(currentInputRID);
				tempInput = input.getInt(tempField);
				
				window.setInt(tempField, tempInput);
				//System.out.println("windowa gelen:" + tempInput);
				
				//System.out.println("inputa gelen:" + tempWindow);
				//input.moveToRid(lastInputRID);
				//input.insert();
				if(iterasyonSay�s� == 0){
					inpList.put(tempField, tempWindow);
				}
				else{
					input.setInt(tempField, tempWindow);
					//System.out.println("ge�emedi");
				}
				
			}
			//input.close();
			if(replacedInwindow != null && !replacedInwindow.contains(maxRID)){
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
