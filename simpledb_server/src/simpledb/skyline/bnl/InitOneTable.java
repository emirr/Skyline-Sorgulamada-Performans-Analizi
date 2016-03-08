package simpledb.skyline.bnl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import simpledb.metadata.MetadataMgr;
import simpledb.record.RecordFile;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class InitOneTable {
	public static int numberOfTuples = 2000;

	public ArrayList<String> skylineFields = new ArrayList<>();
	public static int[][] allTuples;
	public static int[][] skylineTuples;
	public static int[][] temp;
	private int upsize;

	// public static int[][] hazirDizi = {{77 ,95},{97 ,42},{1 ,77},{72 ,49},{81
	// ,14},{39 ,46},{96 ,22},{64 ,42},{64 ,55},{52 ,9},{50 ,35},{20 ,8}};
	public void setSkylineFiels(String skyField) {
		skylineFields.add(skyField);
	}

	public ArrayList<String> getSkylineFields() {
		return skylineFields;
	}

	public void initData(String dbdir) {
		System.out.println("BEGIN INITIALIZATION");
		SimpleDB.init(dbdir);
		if (SimpleDB.fileMgr().isNew()) {
			System.out.println("loading data");
			MetadataMgr md = SimpleDB.mdMgr();
			Transaction tx = new Transaction();

			// create and populate the student table
			Schema sch = new Schema();
///			sch.addIntField("A");
///			sch.addIntField("B");
			for(int i = 0; i<26; i++){
				//System.out.println(" " + i);
			 sch.addIntField(String.valueOf((char)( i + 65)));
			 System.out.println("field name:" + String.valueOf((char)(i + 65)) );
				
			}
			md.createTable("input", sch, tx);
			TableInfo ti = md.getTableInfo("input", tx);

			RecordFile rf = new RecordFile(ti, tx);
			while (rf.next())
				rf.delete();
			rf.beforeFirst();
			allTuples = new int[numberOfTuples][getSkylineFields().size()];
			for (int id = 0; id < numberOfTuples; id++) {
				rf.insert();
				Random _rgen = new Random();
				
				for(int i=0; i<26; i++){
					// sch.addIntField(String.valueOf((char)(i + 65)));
					// System.out.println("field name:" +  );
					int fieldVal = _rgen.nextInt(200);
					rf.setInt(String.valueOf((char)(i + 65)), fieldVal);
//					if(i < getSkylineFields().size())
//						allTuples[id][i] = fieldVal;
//						System.out.print(" " + allTuples[id][i]);
					}

			}
			int a = 0;
			int b = 0;
			//allTuples = new int[numberOfTuples][getSkylineFields().size()];
			rf.beforeFirst();
			while (rf.next()){
				b = 0;
				for (String tempField : getSkylineFields()) {
					//System.out.println(tempField);
					allTuples[a][b] = rf.getInt(tempField);
					b++;
				}
				a++;
			}
			rf.close();
			// for(int i = 0; i < numberOfTuples; i++){
			// skylineTuples[i][0] = 0;
			// }
		
			dummy();
			rf.close();
			tx.commit();
			tx = new Transaction();
			tx.recover(); // add a checkpoint record, to limit rollback
		}
		else{
			MetadataMgr md1 = SimpleDB.mdMgr();
			Transaction tx1 = new Transaction();
			TableInfo ti1 = md1.getTableInfo("input", tx1);

			RecordFile rf1 = new RecordFile(ti1, tx1);
			int a = 0;
			int b = 0;
			allTuples = new int[numberOfTuples][getSkylineFields().size()];
			while (rf1.next()){
				b = 0;
				for (String tempField : getSkylineFields()) {
					//System.out.println(tempField);
					allTuples[a][b] = rf1.getInt(tempField);
					b++;
				}
				a++;
			}
			dummy();
			rf1.close();
			tx1.commit();
			tx1 = new Transaction();
			tx1.recover(); // add a checkpoint record, to limit rollback

		}
	}
	public void dummy(){
		skylineTuples = new int[1][getSkylineFields().size()];
		for(int i = 0; i < getSkylineFields().size(); i++){
			skylineTuples[0][i] = allTuples[0][i];
		//	skylineTuples[0][1] = allTuples[0][1];
			/*System.out.print("" + i + "." +"kayıt:" + allTuples[0][i] + " ");*/
			//System.out.println(allTuples[i][1]);
		}
		/*System.out.println();*/

		for (int i = 1; i < numberOfTuples; i++) {
			int countOfAllTupleDominate = 0;/*
											 * alltuple'daki değer,
											 * skylinetuple'daki değeri
											 * domine etmişse sıfırdan
											 * farklı,bir kere dahi domine
											 * etmesi yeterli
											 */
			int j = 0;
			/*System.out.println();
			System.out.print("" + i + "." +"kayıt:" + allTuples[i][0] + " ");
			System.out.println(allTuples[i][1]);*/
			while (j < skylineTuples.length) {
				//if (allTuples[i][0] < skylineTuples[j][0]) {
				//	if (allTuples[i][1] <= skylineTuples[j][1]) {
						// inputDominationMap.put(window, input);
				/*System.out.println();
				System.out.print("" + j + "." +"skyline kayıt:" + skylineTuples[j][0] + " ");
				System.out.println(skylineTuples[j][1]);*/
				if(compareDomination4Input(allTuples, skylineTuples, i, j) == 1 ){
					if (countOfAllTupleDominate == 0) {
						for (int a = 0; a < getSkylineFields().size(); a++){
							skylineTuples[j][a] = allTuples[i][a];
						}
						countOfAllTupleDominate++;
						j++;
					}

					else {/*
							 * mevcut allTuple değeri daha önce
							 * skylineda bir değeri domine etti bu sefer
							 * bir başka değeri daha domine etti.Bu
							 * nedenle allTuple değerinin domine ettiği
							 * ilk değer dışındaki skyline değerleri
							 * listeden silinmeli.
							 */
						temp = skylineTuples;
						skylineTuples = new int[temp.length - 1][getSkylineFields().size()];
						for (int k = 0; k < j; k++) {
							skylineTuples[k] = Arrays.copyOf(temp[k], temp[k].length);
						}
						for (int k = j; k < temp.length - 1; k++) {
							skylineTuples[k] = Arrays.copyOf(temp[k + 1], temp[k + 1].length);
						}
					}

				}
				
				else {
					   if (compareDomination4Input(allTuples, skylineTuples, i, j) == -1) 
						//if (allTuples[i][1] >= skylineTuples[j][1])
							break;// allTuple'daki değer domine edildi.
						else {// alltuple'daki mevcut değer ve skylinedaki
								// bütün değerler birbirini domine edemedi
//							
							if(j == (skylineTuples.length - 1)){
								if(countOfAllTupleDominate == 0){
									temp = skylineTuples;
									skylineTuples = new int[temp.length +1][getSkylineFields().size()];
									for (int k = 0; k < temp.length; k++) {
										skylineTuples[k] = Arrays.copyOf(temp[k], temp[k].length);
									}
									//temp.length == j + 1
									for (int a = 0; a < getSkylineFields().size(); a++){
										skylineTuples[temp.length][a] = allTuples[i][a];
									}
									// skylineTuples.length++;
									j = j + 2;
								}
								else
									j++;
							}
							else  {
								j++;

							}
						}
					

							
				}
			}

//		
	
//		skylineTuples = new int[1][getSkylineFields().size()];
//		skylineTuples[0][0] = allTuples[0][0];
//		skylineTuples[0][1] = allTuples[0][1];
//
//		for (int i = 1; i < numberOfTuples; i++) {
//			int countOfAllTupleDominate = 0;/*
//											 * alltuple'daki deðer,
//											 * skylinetuple'daki deðeri
//											 * domine etmiþse sýfýrdan
//											 * farklý,birt kere dahi domine
//											 * etmesi yeterli
//											 */
//			int j = 0;
//			while (j < skylineTuples.length) {
//				if (allTuples[i][0] < skylineTuples[j][0]) {
//					if (allTuples[i][1] <= skylineTuples[j][1]) {
//						// inputDominationMap.put(window, input);
//						if (countOfAllTupleDominate == 0) {
//							skylineTuples[j][0] = allTuples[i][0];
//							skylineTuples[j][1] = allTuples[i][1];
//							countOfAllTupleDominate++;
//							j++;
//						}
//
//						else {/*
//								 * mevcut allTuple deðeri daha önce
//								 * skylineda bir deðeri domine etti bu sefer
//								 * bir baþka deðeri daha domine etti.Bu
//								 * nedenle allTuple deðerinin domine ettiði
//								 * ilk deðer dýþýndaki skyline deðerleri
//								 * listeden silinmeli.
//								 */
//							temp = skylineTuples;
//							skylineTuples = new int[temp.length - 1][2];
//							for (int k = 0; k < j; k++) {
//								skylineTuples[k] = Arrays.copyOf(temp[k], temp[k].length);
//							}
//							for (int k = j; k < temp.length - 1; k++) {
//								skylineTuples[k] = Arrays.copyOf(temp[k + 1], temp[k + 1].length);
//							}
//						}
//
//					} else {
//						if(j == (skylineTuples.length - 1)){
//							if(countOfAllTupleDominate == 0){
//								temp = skylineTuples;
//								skylineTuples = new int[temp.length +1][2];
//								for (int k = 0; k < temp.length; k++) {
//									skylineTuples[k] = Arrays.copyOf(temp[k], temp[k].length);
//								}
//								//temp.length == j + 1
//								skylineTuples[temp.length][0] = allTuples[i][0];
//								skylineTuples[temp.length][1] = allTuples[i][1];
//								// skylineTuples.length++;
//								j = j + 2;
//							}
//							else
//								j++;
//						}
//						else  {
//							j++;
//
//						}
//					}
//				} else {
//					if (allTuples[i][0] > skylineTuples[j][0]) {
//						if (allTuples[i][1] >= skylineTuples[j][1])
//							break;// allTuple'daki deðer domine edildi.
//						else {// alltuple'daki mevcut deðer ve skylinedaki
//								// bütün deðerler birbirini domine edemedi
////							if (countOfAllTupleDominate == 0 && j == (skylineTuples.length - 1)) {
////								temp = skylineTuples;
////								skylineTuples = new int[temp.length + 1][2];
////								for (int k = 0; k < temp.length; k++) {
////									skylineTuples[k] = Arrays.copyOf(temp[k], temp[k].length);
////								}
////								//temp.length == j + 1
////								skylineTuples[temp.length][0] = allTuples[i][0];
////								skylineTuples[temp.length][1] = allTuples[i][1];
////								// skylineTuples.length++;
////								j = j + 2;
////
////							}
//							if(j == (skylineTuples.length - 1)){
//								if(countOfAllTupleDominate == 0){
//									temp = skylineTuples;
//									skylineTuples = new int[temp.length +1][2];
//									for (int k = 0; k < temp.length; k++) {
//										skylineTuples[k] = Arrays.copyOf(temp[k], temp[k].length);
//									}
//									//temp.length == j + 1
//									skylineTuples[temp.length][0] = allTuples[i][0];
//									skylineTuples[temp.length][1] = allTuples[i][1];
//									// skylineTuples.length++;
//									j = j + 2;
//								}
//								else
//									j++;
//							}
//							else  {
//								j++;
//
//							}
//						}
//					} else {
//						if (allTuples[i][1] > skylineTuples[j][1])
//							break;// allTuple'daki deðer domine edildi.
//						else {
//							if (allTuples[i][1] < skylineTuples[j][1]) {
//								// inputDominationMap.put(window, input);
//								if (countOfAllTupleDominate == 0) {
//									skylineTuples[j][0] = allTuples[i][0];
//									skylineTuples[j][1] = allTuples[i][1];
//									countOfAllTupleDominate++;
//									j++;
//								} else {/*
//										 * mevcut allTuple deðeri daha önce
//										 * skylineda bir deðeri domine etti
//										 * bu sefer bir baþka deðeri daha
//										 * domine etti.Bu nedenle allTuple
//										 * deðerinin domine ettiði ilk deðer
//										 * dýþýndaki skyline deðerleri
//										 * listeden silinmeli.
//										 */
//									temp = skylineTuples;
//									skylineTuples = new int[temp.length - 1][2];
//									for (int k = 0; k < j; k++) {
//										skylineTuples[k] = Arrays.copyOf(temp[k], temp[k].length);
//									}
//									for (int k = j; k < temp.length - 1; k++) {
//										skylineTuples[k] = Arrays.copyOf(temp[k + 1], temp[k + 1].length);
//									}
//								}
//							}
//
//							else {
////								if (countOfAllTupleDominate == 0 && j == (skylineTuples.length - 1)) {
////									temp = skylineTuples;
////									skylineTuples = new int[temp.length + 1][2];
////									for (int k = 0; k < temp.length; k++) {
////										skylineTuples[k] = Arrays.copyOf(temp[k], temp[k].length);
////									}
////									//temp.length == j + 1
////									skylineTuples[temp.length][0] = allTuples[i][0];
////									skylineTuples[temp.length][1] = allTuples[i][1];
////									j = j + 2;
////								}
//								if(j == (skylineTuples.length - 1)){
//									if(countOfAllTupleDominate == 0){
//										temp = skylineTuples;
//										skylineTuples = new int[temp.length +1][2];
//										for (int k = 0; k < temp.length; k++) {
//											skylineTuples[k] = Arrays.copyOf(temp[k], temp[k].length);
//										}
//										//temp.length == j + 1
//										skylineTuples[temp.length][0] = allTuples[i][0];
//										skylineTuples[temp.length][1] = allTuples[i][1];
//										// skylineTuples.length++;
//										j = j + 2;
//									}
//									else
//										j++;
//								}
//								else  {
//									j++;
//
//								}
//							}
//						}
//					}
//				}
//			}
//
//		}
		
		}	
		System.out.println("olmasý gereken skyline:");
		System.out.println("" + skylineTuples.length + " adet");
		for(int[] mtr : skylineTuples){
			for(int h = 0; h<getSkylineFields().size(); h++){
				System.out.print(" " + mtr[h] + " ");
			}
			System.out.println(" ");
		}
}
	public int compareDomination4Input(int m1[][], int m2[][], int a, int b) {

		int k = 0;
		upsize = getSkylineFields().size();
		for (int i = 0; i < getSkylineFields().size(); i++) {
			k += deepCompare(m1[a][i], m2[b][i]);
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
}