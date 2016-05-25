package simpledb.skyline.rtree;

import static java.sql.Types.INTEGER;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

import simpledb.metadata.MetadataMgr;
import simpledb.record.RecordFile;
import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.skyline.bnl.InitOneTable;
import simpledb.spatialindex.rtree.RTree;
import simpledb.spatialindex.spatialindex.Region;
import simpledb.spatialindex.storagemanager.DiskStorageManager;
import simpledb.spatialindex.storagemanager.IBuffer;
import simpledb.spatialindex.storagemanager.IStorageManager;
import simpledb.spatialindex.storagemanager.PropertySet;
import simpledb.spatialindex.storagemanager.RandomEvictionsBuffer;
import simpledb.tx.Transaction;

public class RTreeLoader {
	RTree tree;
	//Object[][] allTuples;
	// public Object[][] getAllTuples() {
	// return allTuples;
	// }

	public RTree getTree() {
		return tree;
	}

	long treeCreateDiskAccess;

	public long getTreeCreateDiskAccess() {
		return treeCreateDiskAccess;
	}

	Transaction tx = new Transaction();
	MetadataMgr md = SimpleDB.mdMgr();

	/*
	 * input tablosunundaki kayýtlarý sýrayla okuyoruz. Burda 1 tampon(page)
	 * kullanýyor.
	 */

	TableInfo ti;
	RecordFile input;

	public RTreeLoader(String inputFileName, String tmp_rtree, int capacity, int clickCount,
			ArrayList<String> skylineFields, Object[][] allTuples) {
//		allTuples = new Object[InitOneTable.allTuples.length][skylineFields.size()];

		ti = md.getTableInfo(inputFileName, tx);
		input = new RecordFile(ti, tx);

		PropertySet ps = new PropertySet();

		Boolean b = new Boolean(true);
		ps.setProperty("Overwrite", b);
		// overwrite the file if it exists.

		ps.setProperty("FileName", tmp_rtree + clickCount);
		// .idx and .dat extensions will be added.

		Integer i = new Integer(400);
		ps.setProperty("PageSize", i);
		// specify the page size. Since the index may also contain user defined
		// data
		// there is no way to know how big a single node may become. The storage
		// manager
		// will use multiple pages per node if needed. Off course this will slow
		// down performance.

		IStorageManager diskfile = null;
		try {
			diskfile = new DiskStorageManager(ps);
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NullPointerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		IBuffer file = new RandomEvictionsBuffer(diskfile, 10, false);
		// applies a main memory random buffer on top of the persistent storage
		// manager
		// (LRU buffer, etc can be created the same way).

		// Create a new, empty, RTree with dimensionality 2, minimum load 70%,
		// using "file" as
		// the StorageManager and the RSTAR splitting policy.
		PropertySet ps2 = new PropertySet();
		Double f = new Double(0.7);
		ps2.setProperty("FillFactor", f);

		i = new Integer(capacity);
		ps2.setProperty("IndexCapacity", i);
		ps2.setProperty("LeafCapacity", i);
		// Index capacity and leaf capacity may be different.
		i = new Integer(skylineFields.size());
		ps2.setProperty("Dimension", i);

		tree = new RTree(ps2, file);

		int count = 0;
		int indexIO = 0;
		int leafIO = 0;
		// int id;
		// double x1, y1;//iki boyut için, çok boyut için güncellenecek
		double[] f1 = new double[skylineFields.size()];
		double[] f2 = new double[skylineFields.size()];
		// int[] g1 = new int[skylineFields.size()];
		// int[] g2 = new int[skylineFields.size()];

		input.beforeFirst();
		int k = 0;
		while (input.next()) {
			// int i=0;
			int j = 0;
			// int tmp=0;
			double temp = 0.0;
			for (String tempField : skylineFields) {
				// System.out.print(tempField + ":");
				if (ti.schema().type(tempField) == INTEGER) {// eðer tip int ise
																// double'a
																// çevirip ekle
					temp = (double) input.getInt(tempField);
					f1[j] = temp;
					f2[j] = temp;
					allTuples[k][j] = temp;
					j++;
					// System.out.print(" " + input.getInt(tempField) + "-");
				}
				// if (sch.type(tempField) == DOUBLE)
				else {
					temp = input.getDouble(tempField);
					f1[j] = temp;
					f2[j] = temp;
					allTuples[k][j] = temp;
					j++;
					// System.out.print(" " + input.getDouble(tempField) + " ");
				}
				// System.out.print(" " + input.getInt(tempField));

			}
			Region r = new Region(f1, f2);

			String data = r.toString();

			tree.insertData(null, r, k);
			k++;// id
		}
		tree.flush();
		treeCreateDiskAccess = tree.m_stats.getReads();
		System.out.println("treeoluþturmak için disk eriþim:" + treeCreateDiskAccess);

	}

}
