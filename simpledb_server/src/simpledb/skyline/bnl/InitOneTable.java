package simpledb.skyline.bnl;

import static java.sql.Types.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import org.apache.commons.math3.distribution.MultivariateNormalDistribution;

import simpledb.metadata.MetadataMgr;
import simpledb.record.RecordFile;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class InitOneTable {
	public int numberOfTuples;
	private String distributionType;
	private String tableName;
	// private String dbName;
	MetadataMgr md, md1;
	Transaction tx, tx1;
	Schema sch;// bu sınıfın bütün objelerinde aynı schema bilgisi olsun.
	TableInfo ti, ti1;
	RecordFile rf, rf1;
	public ArrayList<String> skylineFields;
	public static Object[][] allTuples;
	public static Object[][] skylineTuples;
	public static Object[][] temp;
	private int upsize;
	private int buffSize;

	public InitOneTable(int tupleSize, String distType, String tblName, Schema sch, String dbName, int bufferSize,
			int click) {
		this.numberOfTuples = tupleSize;
		this.distributionType = distType;
		this.tableName = tblName;
		// this.dbName = dbName;
		this.sch = sch;
		// this.skylineFields = skylineFields;
		this.buffSize = bufferSize;
		// System.out.println("nee");
		initData(dbName, click);
	}

	public InitOneTable(String tblName, Schema sch) {
		// this.numberOfTuples = tupleSize;
		this.tableName = tblName;
		this.sch = sch;
	}

	// public static int[][] hazirDizi = {{77 ,95},{97 ,42},{1 ,77},{72 ,49},{81
	// ,14},{39 ,46},{96 ,22},{64 ,42},{64 ,55},{52 ,9},{50 ,35},{20 ,8}};
	public void setSkylineFiels(ArrayList<String> skyField) {
		// skylineFields.add(skyField);
		skylineFields = skyField;
	}

	public void setBuffSize(int size) {
		buffSize = size;
	}

	public ArrayList<String> getSkylineFields() {
		return skylineFields;
	}

	// public void createTable()
	public void initData(String dbdir, int tableCount) {
		System.out.println("BEGIN INITIALIZATION");
		// SimpleDB.setBUFFER_SIZE(buffSize);
		if (tx != null)
			tx.commit();
		if (tableCount == 0)
			SimpleDB.init(dbdir);

		// System.out.println("burası mı?");
		md = SimpleDB.mdMgr();
		tx = new Transaction();

		// addField();
		System.out.println("initTableName:" + tableName);
		// && click == 0
		if (tableName != null) {
			// System.out.println("loading data");

			md.createTable(tableName, sch, tx);
			ti = md.getTableInfo(tableName, tx);

			// md = SimpleDB.mdMgr();
			// tx = new Transaction();

			// create and populate the student table
			// sch = new Schema();
			/// sch.addIntField("A");
			/// sch.addIntField("B");

			// addField();
			// md.createTable(tableName, sch, tx);
			// ti = md.getTableInfo(tableName, tx);
			System.out.println("yazacak:" + ti.fileName());
			rf = new RecordFile(ti, tx);
			while (rf.next())
				rf.delete();
			rf.beforeFirst();
			if (distributionType.equals("indp.")) {

				for (int id = 0; id < numberOfTuples; id++) {
					rf.insert();
					Random _rgen = new Random();

					for (String schField : ti.schema().fields()) {
						//System.out.println(" ");
						//System.out.print("fieldname:" + schField + " ");
						if (sch.type(schField) == INTEGER) {
							int fieldVal = _rgen.nextInt(200) + 10;

							rf.setInt(schField, fieldVal);
							//System.out.print("" + rf.getInt(schField) + " ");
						} else {
							if (sch.type(schField) == VARCHAR) {
								String fieldValStr = new String();
								rf.setString(schField, fieldValStr);
								//System.out.print("" + rf.getString(schField) + " ");
								// System.out.println(" string yok ki");
							} else {
								double fieldVal1 = 10 + _rgen.nextDouble() * 200.0;
								rf.setDouble(schField, fieldVal1);
								//System.out.print(" " + rf.getDouble(schField));
							}

						}
					}

				}

				rf.close();
				tx.commit();
				// tx = new Transaction();
				// tx.recover(); // add a checkpoint record, to limit rollback
			} else {
				if (distributionType.equals("corr")) {
					int stringSayisi = 0;
					for (String schemaNames : ti.schema().fields()) {
						if (sch.type(schemaNames) == VARCHAR)
							stringSayisi++;
					}
					// rf.insert();
					double[] means = new double[ti.schema().fields().size() - stringSayisi];
					double[][] cov = new double[ti.schema().fields().size() - stringSayisi][ti.schema().fields().size()
							- stringSayisi];
					MultivariateNormalDistribution mnd;
					double[] valuesss = new double[ti.schema().fields().size() - stringSayisi];
					double[][] list = new double[numberOfTuples][ti.schema().fields().size() - stringSayisi];

					for (int i = 0; i < list.length; i++) {
						rf.insert();
						System.out.println("" + i + "." + "kay�t");
						Random _rgen = new Random();
						int a = _rgen.nextInt(190) + 50;

						for (int k = 0; k < means.length; k++) {
							for (int l = 0; l < means.length; l++) {
								if (k == l) {

									cov[k][l] = 100 + _rgen.nextDouble() * 300.0;
								} else {
									// System.out.println("l:" + l + "k:" + k);
									if (cov[l][k] != 0)
										cov[k][l] = cov[l][k];
									else
										cov[k][l] = 10 + _rgen.nextDouble() * 30.0;
								}
							}
						}

						double c = (double) a;
						//System.out.println("c:" + c);
						for (int u = 0; u < means.length; u++) {
							means[u] = c;
						}
						// means[0] = c;
						// means[1] = c;
						// means[2] = c;
						// means[3] = c;
						mnd = new MultivariateNormalDistribution(means, cov);
						// mnd.
						valuesss = mnd.sample();
						// for (int j = 0; j < means.length; j++) {
						// if (valuesss[j] < 0)
						// list[i][j] = -valuesss[j];
						// else
						// list[i][j] = valuesss[j];
						int valIndx = 0;

						for (String schField : ti.schema().fields()) {
							//System.out.println(" ");
							//System.out.print("fieldname:" + schField + " ");
							if (sch.type(schField) == INTEGER) {
								if (valuesss[valIndx] < 0)
									valuesss[valIndx] = -valuesss[valIndx];

								int tmpValInt = (int) valuesss[valIndx];
								rf.setInt(schField, tmpValInt);
								valIndx++;
								//System.out.print("" + rf.getInt(schField) + " ");
							} else {
								if (sch.type(schField) == VARCHAR) {
									String fieldValStr = new String();
									rf.setString(schField, fieldValStr);
									System.out.print("" + rf.getInt(schField) + " ");
									// System.out.println(" string yok ki");
								} else {
									if (valuesss[valIndx] < 0)
										valuesss[valIndx] = -valuesss[valIndx];
									rf.setDouble(schField, valuesss[valIndx]);
									valIndx++;
									//System.out.print(" " + rf.getDouble(schField) + " ");
								}

							}
						}
						//System.out.println("valINdex:" + valIndx);
						// }
					}
					// System.out.println();
					//
					// for (double[] value : list) {
					// System.out.print(" " + value[0]);
					// System.out.println(" " + value[1]);
					// }
					rf.close();
					tx.commit();
				}

				// }
			}

		}
	}

	public void dummy() {

		md1 = SimpleDB.mdMgr();
		tx1 = new Transaction();
		ti1 = md1.getTableInfo(tableName, tx1);
		rf1 = new RecordFile(ti1, tx1);
		// allTuples = new int[numberOfTuples][getSkylineFields().size()];

		int g = 0;
		int b = 0;
		int recordSize = 0;
		System.out.println("");
		while (rf1.next()) {
			recordSize++;
		}
		System.out.println("alltuple size:" + recordSize);
		allTuples = new Object[recordSize][getSkylineFields().size()];
		rf1.beforeFirst();
		while (rf1.next()) {
			b = 0;
			for (String tempField : getSkylineFields()) {
				// // System.out.println(tempField);
				if (sch.type(tempField) == INTEGER)
					allTuples[g][b] = rf1.getInt(tempField);
				else
					allTuples[g][b] = rf1.getDouble(tempField);

				b++;
			}
			g++;
			// RecordFile rf2 = rf1;
			// if(rf2.next()){
			// temp = allTuples;
			// allTuples = new int[temp.length + 1][getSkylineFields().size()];
			// for (int k = 0; k < temp.length; k++) {
			// allTuples[k] = Arrays.copyOf(temp[k], temp[k].length);
			// }
			//// // temp.length == j + 1
			////// for (int a = 0; a < getSkylineFields().size(); a++) {
			////// allTuples[temp.length][a] = allTuples[i][a];
			////// }
			//// g++;
			//// }
			//
			// }
		}
		System.out.println("alltuple size:" + allTuples.length);

		rf1.close();
		tx1.commit();
		// tx1 = new Transaction();
		// tx1.recover(); // add a checkpoint record, to limit rollback
		skylineTuples = new Object[1][getSkylineFields().size()];
		for (int i = 0; i < getSkylineFields().size(); i++) {
			skylineTuples[0][i] = allTuples[0][i];
			// skylineTuples[0][1] = allTuples[0][1];
			/*
			 * System.out.print("" + i + "." +"kayıt:" + allTuples[0][i] + " "
			 * );
			 */
			// System.out.println(allTuples[i][1]);
		}
		/* System.out.println(); */

		for (int i = 1; i < recordSize; i++) {
			int countOfAllTupleDominate = 0;/*
											 * alltuple'daki değer,
											 * skylinetuple'daki değeri domine
											 * etmişse sıfırdan farklı,bir
											 * kere dahi domine etmesi yeterli
											 */
			int j = 0;
			/*
			 * System.out.println(); System.out.print("" + i + "." +"kayıt:" +
			 * allTuples[i][0] + " "); System.out.println(allTuples[i][1]);
			 */
			while (j < skylineTuples.length) {
				// if (allTuples[i][0] < skylineTuples[j][0]) {
				// if (allTuples[i][1] <= skylineTuples[j][1]) {
				// inputDominationMap.put(window, input);
				/*
				 * System.out.println(); System.out.print("" + j + "." +
				 * "skyline kayıt:" + skylineTuples[j][0] + " ");
				 * System.out.println(skylineTuples[j][1]);
				 */
				if (compareDomination4Input(allTuples, skylineTuples, i, j) == 1) {
					if (countOfAllTupleDominate == 0) {
						for (int a = 0; a < getSkylineFields().size(); a++) {
							skylineTuples[j][a] = allTuples[i][a];
						}
						countOfAllTupleDominate++;
						j++;
					}

					else {/*
							 * mevcut allTuple değeri daha önce skylineda bir
							 * değeri domine etti bu sefer bir başka değeri
							 * daha domine etti.Bu nedenle allTuple değerinin
							 * domine ettiği ilk değer dışındaki skyline
							 * değerleri listeden silinmeli.
							 */
						temp = skylineTuples;
						skylineTuples = new Object[temp.length - 1][getSkylineFields().size()];
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
						// if (allTuples[i][1] >= skylineTuples[j][1])
						break;// allTuple'daki değer domine edildi.
					else {// alltuple'daki mevcut değer ve skylinedaki
							// bütün değerler birbirini domine edemedi
							//
						if (j == (skylineTuples.length - 1)) {
							if (countOfAllTupleDominate == 0) {
								temp = skylineTuples;
								skylineTuples = new Object[temp.length + 1][getSkylineFields().size()];
								for (int k = 0; k < temp.length; k++) {
									skylineTuples[k] = Arrays.copyOf(temp[k], temp[k].length);
								}
								// temp.length == j + 1
								for (int a = 0; a < getSkylineFields().size(); a++) {
									skylineTuples[temp.length][a] = allTuples[i][a];
								}
								// skylineTuples.length++;
								j = j + 2;
							} else
								j++;
						} else {
							j++;

						}
					}

				}
			}

		}
		System.out.println("olmasý gereken skyline:");
		System.out.println("" + skylineTuples.length + " adet");
		for (Object[] mtr : skylineTuples) {
			for (int h = 0; h < getSkylineFields().size(); h++) {
				System.out.print(" " + mtr[h] + " ");
			}
			System.out.println(" ");
		}
	}

	public int compareDomination4Input(Object m1[][], Object m2[][], int a, int b) {

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
		} else {
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
		}
		return m;
	}
}