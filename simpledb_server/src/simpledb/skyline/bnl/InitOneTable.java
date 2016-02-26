package simpledb.skyline.bnl;

import java.util.Arrays;
import java.util.Random;

import simpledb.metadata.MetadataMgr;
import simpledb.record.RecordFile;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class InitOneTable {
	public static int numberOfTuples = 300;
	

	public static int[][] allTuples = new int[numberOfTuples][2];
	public static int[][] skylineTuples = new int[1][2];
	public static int[][] temp;
	//public static int[][] hazirDizi = {{77 ,95},{97 ,42},{1 ,77},{72 ,49},{81 ,14},{39 ,46},{96 ,22},{64 ,42},{64 ,55},{52 ,9},{50 ,35},{20 ,8}}; 

	public static void initData(String dbdir) {
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
			for (int id = 0; id < numberOfTuples; id++) {
				rf.insert();
				Random _rgen = new Random();
				
				for(int i=0; i<26; i++){
					// sch.addIntField(String.valueOf((char)(i + 65)));
					// System.out.println("field name:" +  );
					int fieldVal = _rgen.nextInt(200);
					rf.setInt(String.valueOf((char)(i + 65)), fieldVal);
					if(i < 2)
						allTuples[id][i] = fieldVal;
					}
//				int _A = _rgen.nextInt(100);
//			//	int _A = hazirDizi[id][0];
//				System.out.print(" " + _A);
//				rf.setInt("A", _A);
//				allTuples[id][0] = _A;
//				int _B = _rgen.nextInt(100);
//			//	int _B = hazirDizi[id][1];
//				System.out.println(" " + _B);
//				rf.setInt("B", _B);
//				allTuples[id][1] = _B;
			}

			rf.close();
			// for(int i = 0; i < numberOfTuples; i++){
			// skylineTuples[i][0] = 0;
			// }
			skylineTuples[0][0] = allTuples[0][0];
			skylineTuples[0][1] = allTuples[0][1];

			for (int i = 1; i < numberOfTuples; i++) {
				int countOfAllTupleDominate = 0;/*
												 * alltuple'daki deðer,
												 * skylinetuple'daki deðeri
												 * domine etmiþse sýfýrdan
												 * farklý,birt kere dahi domine
												 * etmesi yeterli
												 */
				int j = 0;
				while (j < skylineTuples.length) {
					if (allTuples[i][0] < skylineTuples[j][0]) {
						if (allTuples[i][1] <= skylineTuples[j][1]) {
							// inputDominationMap.put(window, input);
							if (countOfAllTupleDominate == 0) {
								skylineTuples[j][0] = allTuples[i][0];
								skylineTuples[j][1] = allTuples[i][1];
								countOfAllTupleDominate++;
								j++;
							}

							else {/*
									 * mevcut allTuple deðeri daha önce
									 * skylineda bir deðeri domine etti bu sefer
									 * bir baþka deðeri daha domine etti.Bu
									 * nedenle allTuple deðerinin domine ettiði
									 * ilk deðer dýþýndaki skyline deðerleri
									 * listeden silinmeli.
									 */
								temp = skylineTuples;
								skylineTuples = new int[temp.length - 1][2];
								for (int k = 0; k < j; k++) {
									skylineTuples[k] = Arrays.copyOf(temp[k], temp[k].length);
								}
								for (int k = j; k < temp.length - 1; k++) {
									skylineTuples[k] = Arrays.copyOf(temp[k + 1], temp[k + 1].length);
								}
							}

						} else {
							if(j == (skylineTuples.length - 1)){
								if(countOfAllTupleDominate == 0){
									temp = skylineTuples;
									skylineTuples = new int[temp.length +1][2];
									for (int k = 0; k < temp.length; k++) {
										skylineTuples[k] = Arrays.copyOf(temp[k], temp[k].length);
									}
									//temp.length == j + 1
									skylineTuples[temp.length][0] = allTuples[i][0];
									skylineTuples[temp.length][1] = allTuples[i][1];
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
					} else {
						if (allTuples[i][0] > skylineTuples[j][0]) {
							if (allTuples[i][1] >= skylineTuples[j][1])
								break;// allTuple'daki deðer domine edildi.
							else {// alltuple'daki mevcut deðer ve skylinedaki
									// bütün deðerler birbirini domine edemedi
//								if (countOfAllTupleDominate == 0 && j == (skylineTuples.length - 1)) {
//									temp = skylineTuples;
//									skylineTuples = new int[temp.length + 1][2];
//									for (int k = 0; k < temp.length; k++) {
//										skylineTuples[k] = Arrays.copyOf(temp[k], temp[k].length);
//									}
//									//temp.length == j + 1
//									skylineTuples[temp.length][0] = allTuples[i][0];
//									skylineTuples[temp.length][1] = allTuples[i][1];
//									// skylineTuples.length++;
//									j = j + 2;
//
//								}
								if(j == (skylineTuples.length - 1)){
									if(countOfAllTupleDominate == 0){
										temp = skylineTuples;
										skylineTuples = new int[temp.length +1][2];
										for (int k = 0; k < temp.length; k++) {
											skylineTuples[k] = Arrays.copyOf(temp[k], temp[k].length);
										}
										//temp.length == j + 1
										skylineTuples[temp.length][0] = allTuples[i][0];
										skylineTuples[temp.length][1] = allTuples[i][1];
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
						} else {
							if (allTuples[i][1] > skylineTuples[j][1])
								break;// allTuple'daki deðer domine edildi.
							else {
								if (allTuples[i][1] < skylineTuples[j][1]) {
									// inputDominationMap.put(window, input);
									if (countOfAllTupleDominate == 0) {
										skylineTuples[j][0] = allTuples[i][0];
										skylineTuples[j][1] = allTuples[i][1];
										countOfAllTupleDominate++;
										j++;
									} else {/*
											 * mevcut allTuple deðeri daha önce
											 * skylineda bir deðeri domine etti
											 * bu sefer bir baþka deðeri daha
											 * domine etti.Bu nedenle allTuple
											 * deðerinin domine ettiði ilk deðer
											 * dýþýndaki skyline deðerleri
											 * listeden silinmeli.
											 */
										temp = skylineTuples;
										skylineTuples = new int[temp.length - 1][2];
										for (int k = 0; k < j; k++) {
											skylineTuples[k] = Arrays.copyOf(temp[k], temp[k].length);
										}
										for (int k = j; k < temp.length - 1; k++) {
											skylineTuples[k] = Arrays.copyOf(temp[k + 1], temp[k + 1].length);
										}
									}
								}

								else {
//									if (countOfAllTupleDominate == 0 && j == (skylineTuples.length - 1)) {
//										temp = skylineTuples;
//										skylineTuples = new int[temp.length + 1][2];
//										for (int k = 0; k < temp.length; k++) {
//											skylineTuples[k] = Arrays.copyOf(temp[k], temp[k].length);
//										}
//										//temp.length == j + 1
//										skylineTuples[temp.length][0] = allTuples[i][0];
//										skylineTuples[temp.length][1] = allTuples[i][1];
//										j = j + 2;
//									}
									if(j == (skylineTuples.length - 1)){
										if(countOfAllTupleDominate == 0){
											temp = skylineTuples;
											skylineTuples = new int[temp.length +1][2];
											for (int k = 0; k < temp.length; k++) {
												skylineTuples[k] = Arrays.copyOf(temp[k], temp[k].length);
											}
											//temp.length == j + 1
											skylineTuples[temp.length][0] = allTuples[i][0];
											skylineTuples[temp.length][1] = allTuples[i][1];
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
					}
				}

			}
			System.out.println("olmasý gereken skyline:");
			System.out.println("" + skylineTuples.length + " adet");
			for(int[] mtr : skylineTuples)
				System.out.println(" " + mtr[0] + " " + mtr[1]);
			rf.close();
			tx.commit();
			tx = new Transaction();
			tx.recover(); // add a checkpoint record, to limit rollback
		}
	}
}