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
	private RecordFile input;
	private int diskerisimsayisi;
	private RID windRID, inpRID;
	private int id, blkNo;
	private ArrayList<String> skylineDimensions;
//	private int tempRecordSize;// bu silinmeli 4...
//	private static int k, l;
	private int upsize;
	private Map<RID, RID> inputDominationMap = new HashMap<>();
	static ArrayList<Integer> skylinePoints = new ArrayList<>();

	SkylineFinder(ArrayList<String> skylineDimension) {
		this.skylineDimensions = skylineDimension;
	}

	public ArrayList<String> getSkylineDimensions() {
		return skylineDimensions;
	}

	/*
	 * input windowda karþýlaþtýrýldýðý deðeri domine ediyorsa 1, input domine
	 * ediliyorsa -1, ikisi de domine edilemiyorsa 0 döndürülür
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
		/*System.out.println("inputdan windowa:" + k++);*/
		for (String tempField : getSkylineDimensions()) {
			// System.out.println(tempField);
			window.setInt(tempField, input.getInt(tempField));

			/*System.out.print(" " + input.getInt(tempField));*/

		}
		/*System.out.println(" ");*/
		// System.out.println(" " + window.getInt("A"));
		// window.setInt("B", input.getInt("B"));
		// System.out.println(" " + window.getInt("B"));
		// long timestamp = System.currentTimeMillis();

		// System.out.print(" " + input.getInt("A"));
		// System.out.println(" " + input.getInt("B"));

	}

	void putRecordToTemp() {
		// tempFile.setInt("A", input.getInt("A"));
		// tempFile.setInt("B", input.getInt("B"));
		//tempRecordSize++;
	/*System.out.println(" " + tempRecordSize + "." + "kayýt tempFile'da");*/
		for (String tempField : getSkylineDimensions()) {
			// System.out.println(tempField);

			tempFile.setInt(tempField, input.getInt(tempField));
			/*System.out.print(" " + tempFile.getInt(tempField));*/
		}

		/*System.out.println(" ");*/
		// System.out.println("inputdan tempFile'a");
		// System.out.print(" " + tempFile.getInt("A"));
		// System.out.print(" " + tempFile.getInt("B"));
		// System.out.print(" " + timestamp );

	}

	/*
	 * false dönerse mevcut input deðeri windowdaki bir deðeri daha önce domine
	 * etmemiþ demektir.Aksi durumda ise input deðeri daha önce bir window
	 * deðerini domine etmiþ demektir.
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
	 * inputDominationMap bir inputa ait domine etme bilgisi taþýr.Bu nedenle
	 * her yeni input için sýfýrlanmasý gerekir.
	 */
	public void clearInputDominationMap() {
		inputDominationMap.clear();
	}

	/* windowu selforganizer olarak yeniden düzenler. */
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
			
			if(window.getRid().equals(new RID(0,0))){//windowun 0.blok 0.slotu boþ deðil demektir.
				for (String tempField : getSkylineDimensions()) {

//					tmpValues = window.getInt(tempField);//0.b 1.slot deðeri
//					window.moveToRid(new RID(0, 0));
//					tmpValues1 = window.getInt(tempField);//0-0 dðri
//					window.setInt(tempField, tmpValues);
//
//					window.moveToRid(new RID(blkNo, id));
//					window.setInt(tempField, tmpValues1);
					tmpValues1 = window.getInt(tempField);
					window.moveToRid(new RID(blkNo, id));
					tmpValues = window.getInt(tempField);
					window.setInt(tempField, tmpValues1);//0.b-1.s ye 0-0 deðeri atandý.
					window.moveToRid(new RID(0, 0));
					window.setInt(tempField, tmpValues);//0.b-0.s a 0-1 deðeri atandý.
				}
			}
			else{//0-0 boþ demektir.
				
//				window.beforeFirst();
//				window.insert();
				for (String tempField : getSkylineDimensions()){
					window.moveToRid(new RID(blkNo, id));
					tmpValues = window.getInt(tempField);
					window.moveToRid(new RID(0, 0));
					window.setInt(tempField, tmpValues);
				//window.moveToRid(new RID(blkNo, id));
				
				}
				window.insert();//0-0 INUSE yapýldý.
				window.moveToRid(new RID(blkNo, id));
				window.delete();//0-1 EMPTY yapýldý.
			}
			

		}
		return replacedInWindow;
	}
}
