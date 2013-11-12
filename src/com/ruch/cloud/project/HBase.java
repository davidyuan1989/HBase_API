package com.ruch.cloud.project;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.generated.master.table_jsp;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.metrics2.filter.RegexFilter;

public class HBase {
	private final static byte[] cellData = Bytes.toBytes("cell_data");
	//public static final String MASTER_IP = "ec2-50-19-6-59.compute-1.amazonaws.com";
	public static final String MASTER_IP = "10.203.22.49";
	// public static final String ZOOKEEPER_PORT = "2181";
	public static final String ZOOKEEPER_PORT = "2181";
	/** Drop tables if this value is set true. */

	private static void p(String msg) {
		System.out.println(msg);
	}

	/**
	 * <table>
	 * timeTable
	 * <tr>
	 * <tb>bucket|createTime|ID(00|20131111000000|XXXXXXX)</tb> <tb>tw:txt</tb>
	 * </tr>
	 * </table>
	 */
	/**
	 * <table>
	 * userTable
	 * <tr>
	 * <tb>bucket|user_id|ID(00|XXXX|XXXXXXX)</tb> <tb>tw:uid</tb>
	 * <tb>tw:r_id</tb> <tb>tw:r_uid</tb><tb>tw:r_st</tb>
	 * </tr>
	 * </table>
	 */

	private byte[] table, row, family, qualifier0, qualifier1, qualifier2,
			qualifier3; //
	private String startBucket, endBucket;
	private String tableName;

	private Configuration config = HBaseConfiguration.create();

	public HBase(String TableName) {
		tableName = TableName;
		if (TableName.equals("timeTable")) {
			table = Bytes.toBytes(TableName);
			qualifier1 = Bytes.toBytes("txt");
			family = Bytes.toBytes("tw");
		} else if (TableName.equals("userTable")) {
			table = Bytes.toBytes(TableName);
			qualifier0 = Bytes.toBytes("uid");
			qualifier1 = Bytes.toBytes("r_id");
			qualifier2 = Bytes.toBytes("r_uid");
			qualifier3 = Bytes.toBytes("r_st");
			family = Bytes.toBytes("tw");
		}
		startBucket = "00";
		endBucket = "09";
	}

	private HBaseAdmin connectionHBase() throws IOException {

		config.set("hbase.zookeeper.quorum", MASTER_IP);
		config.set("hbase.zookeeper.property.clientPort", ZOOKEEPER_PORT);
		p("Running connecting test...");
		try {
			HBaseAdmin admin = new HBaseAdmin(config);
			p("HBase found!");

			return admin;
		} catch (MasterNotRunningException e) {
			p("HBase connection failed!");
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			p("Zookeeper connection failed!");
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}

	private void createTable(HBaseAdmin admin) throws IOException {
		HTableDescriptor desc = new HTableDescriptor(table);
		desc.addFamily(new HColumnDescriptor(family));
		admin.createTable(desc);
	}

	private void deleteTable(HBaseAdmin admin) throws IOException {
		if (admin.tableExists(table)) {
			admin.disableTable(table);
			try {
				admin.deleteTable(table);
			} finally {
			}
		}
	}

	private void filters(HBaseAdmin admin, HTableInterface table)
			throws IOException {
		p("\n*** FILTERS ~ scanning with filters to fetch a row of which key is larget than \"Row1\"~ ***");
		Filter filter1 = new PrefixFilter(row);
		Filter filter2 = new QualifierFilter(CompareOp.GREATER_OR_EQUAL,
				new BinaryComparator(qualifier1));

		List<Filter> filters = Arrays.asList(filter1, filter2);
		Filter filter3 = new FilterList(Operator.MUST_PASS_ALL, filters);

		Scan scan = new Scan();
		scan.setFilter(filter3);

		ResultScanner scanner = table.getScanner(scan);
		try {
			int i = 0;
			for (Result result : scanner) {
				p("Filter " + scan.getFilter() + " matched row: " + result);
				i++;
			}
			assert i == 1 : "This filtering sample should return 1 row but was "
					+ i + ".";
		} finally {
			scanner.close();
		}
		p("Done. ");
	}

	private void get(HBaseAdmin admin, HTableInterface table)
			throws IOException {
		p("\n*** GET example ~fetching the data in Family1:Qualifier1~ ***");
		
		row = Bytes.toBytes("00|20131022094648|392587835876052992");
		Get g = new Get(row);
		Result r = table.get(g);
		byte[] value = r.getValue(family, qualifier1);
		String rowId = Bytes.toString(r.getRow());
		
		p("Fetched value: " + Bytes.toString(value) + "ID:" + rowId);
		//assert Arrays.equals(cellData, value);
		p("Done. ");
	}

	private void put(HBaseAdmin admin, HTableInterface table)
			throws IOException {
		p("\n*** PUT example ~inserting \"cell-data\" into Family1:Qualifier1 of Table1 ~ ***");

		// Row1 => Family1:Qualifier1, Family1:Qualifier2
		Put p = new Put(row);
		p.add(family, qualifier1, cellData);
		p.add(family, qualifier2, cellData);
		table.put(p);

		admin.disableTables(tableName);

		try {
			HColumnDescriptor desc = new HColumnDescriptor(row);
			admin.addColumn(tableName, desc);
			p("Success.");
		} catch (Exception e) {
			p("Failed.");
		} finally {
			admin.enableTable(tableName);
		}
		p("Done. ");
	}

	private String scanRange(HBaseAdmin admin, HTableInterface table,
			String startId, String endId) throws IOException {
		/** Scan with user_id range */
		p("\n*** SCAN example ~fetching data in startId--endId; ***");

		List<Filter> filters = new ArrayList<Filter>();

		String format = String.format("^\\d{2}\\|%s.*", startId.substring(0, 5));
		RegexStringComparator keyRegEx = new RegexStringComparator(format);
		RowFilter rowFilter = new RowFilter(CompareOp.EQUAL, keyRegEx);
		filters.add(rowFilter);

		SingleColumnValueFilter colValFilter_greater = new SingleColumnValueFilter(
				family, qualifier0, CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(startId));
		filters.add(colValFilter_greater);

		SingleColumnValueFilter colValFilter_less = new SingleColumnValueFilter(
				family, qualifier0, CompareOp.LESS_OR_EQUAL, Bytes.toBytes(endId));
		filters.add(colValFilter_less);

		FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL,filters);

		Scan scan = new Scan(Bytes.toBytes(startBucket + "|"
				+ startId.subSequence(0, 5)), Bytes.toBytes(endBucket + "|"
				+ endId.subSequence(0, 5) + new byte[] { 0 }));
		scan.setFilter(fl);

		ResultScanner scanner = table.getScanner(scan);
		int count = 0;
		try {
			
			//for (Result rs = scanner.next(); rs != null; rs = scanner.next()) {
			for(Result rs : scanner){
				p(Bytes.toString(rs.getRow()));
				count++;
			}
			
			p("Count: " + count);
		} finally {
			scanner.close();
		}
		p("Done.");
		return String.valueOf(count);
	}

	private String scanTime(HBaseAdmin admin, HTableInterface table, String time)
			throws IOException {
		/** Scan with same time */

		String name = String.format("^\\d{2}\\|%s\\|.*", time);

		RegexStringComparator keyRegEx = new RegexStringComparator(name);
		RowFilter rowFilter = new RowFilter(CompareOp.EQUAL, keyRegEx);

		p("\n*** SCAN . Rowkey contains time, family: tweet, qualifier1: text  ***");
		Scan scanner = new Scan(Bytes.toBytes(startBucket + "|" + time + "|"),
				Bytes.toBytes(endBucket + "|" + time + "|"));
		scanner.addColumn(family, qualifier1);
		scanner.setFilter(rowFilter);

		ResultScanner resultScanner = table.getScanner(scanner);
		String reString = "";
		try {
			for (Result result : resultScanner) {
				p("row: " + Bytes.toString(result.getRow()));
				p("column:" + result.getColumn(family, qualifier1).toString());
				String[] idstr = Bytes.toString(result.getRow()).split("\\|");
				p(idstr[0]+idstr[1]+idstr[2]);
				reString += idstr[2]+":";
				String textString = Bytes.toString(result.getValue(family, qualifier1));
				reString += textString+"\n";
			}

		} finally {
			resultScanner.close();
		}
		p("Done.");
		return reString;
	}

	private String scanRetweet(HBaseAdmin admin, HTableInterface table,
			String userId) throws IOException {

		SingleColumnValueFilter colValFilter = new SingleColumnValueFilter(
				family, qualifier2, CompareOp.EQUAL, Bytes.toBytes(userId));
		colValFilter.setFilterIfMissing(true);

		p("\n*** SCAN . Rowkey contains time, family: tweet, qualifier1: text  ***");
		Scan uidScanner = new Scan();
		uidScanner.setFilter(colValFilter);

		ResultScanner scannerResult = table.getScanner(uidScanner);
		String reString = "";
		try {
			for (Result result : scannerResult){
				p("Found row: " + Bytes.toString(result.getRow()));
				String []idstr = Bytes.toString(result.getRow()).split("\\|");
				reString += idstr[2]+"\n";
			}
		} finally {
			scannerResult.close();
		}
		p("Done.");
		return reString;

	}

	public void run() throws IOException {
		HBaseAdmin admin = connectionHBase();
		if (admin == null) {
			p("Connection fail...");
			return;
		}
		HTableFactory factory = new HTableFactory();

		HTableInterface Htable = factory.createHTableInterface(config, table);
		
		get(admin,Htable);
		// put(admin, Htable);
		// get(admin, Htable);
		// scanUidRange(admin, Htable, "startId", "endId");
		// scanTime(admin, Htable, "time");
		// scanRetweet(admin, Htable, "userId");
		// filters(admin, Htable);
		// delete(admin, table);
		factory.releaseHTableInterface(Htable); // Disconnect
	}

	public String ServeScanRetweet(String userId) throws IOException {
		HBaseAdmin admin = connectionHBase();
		if (admin == null) {
			p("Connection fail...");
			return "Connection fail...";
		}
		HTableFactory factory = new HTableFactory();

		HTableInterface Htable = factory.createHTableInterface(config, table);
		String res = scanRetweet(admin, Htable, userId);
		factory.releaseHTableInterface(Htable);
		return res;
	}

	public String ServeScanTime(String time) throws IOException {
		HBaseAdmin admin = connectionHBase();
		if (admin == null) {
			p("Connection fail...");
			return "Connection fail...";
		}
		HTableFactory factory = new HTableFactory();

		HTableInterface Htable = factory.createHTableInterface(config, table);
		String res = scanTime(admin, Htable, time);
		factory.releaseHTableInterface(Htable);
		return res;

	}

	public String scanUidRange(String start_id, String end_id) throws IOException {
		HBaseAdmin admin = connectionHBase();
		if (admin == null) {
			p("Connection fail...");
			return "Connection fail...";
		}
		HTableFactory factory = new HTableFactory();
		
		HTableInterface Htable = factory.createHTableInterface(config, table);
		String res = scanRange(admin, Htable, start_id, end_id);
		factory.releaseHTableInterface(Htable);
		
		return res;
	}

	public static void main(String[] args) {
		HBase hBase = new HBase("timeTable");
		HBase hBase2 = new HBase("userTable");
		try {
			p("p2:");
			p(hBase.ServeScanTime("20131022094648"));
			p("p3:");
			p(hBase2.scanUidRange("1887673303","1934878657"));
			p("p4:");
			p(hBase2.ServeScanRetweet("1887673303"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
