package net.gywn;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.apache.shardingsphere.shardingjdbc.api.yaml.YamlShardingDataSourceFactory;

import net.gywn.common.IDGenerator;
import net.gywn.common.RowModifier;
import net.gywn.common.TargetTable;
import net.gywn.algorithm.IDGeneratorHandler;
import net.gywn.algorithm.RowModifierHandler;
import net.gywn.common.Config;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import static picocli.CommandLine.*;

public class Main implements Callable<Integer> {

	public static Config CONFIG;
	public static Random RAND = new Random();

	@Option(names = { "--config-file" }, description = "Config file", required = true)
	private String configFile;

	@Option(names = { "--workers" }, description = "Wokers for loading target")
	private Integer workers;

	@Option(names = { "--source-sharding-config" }, description = "Source sharding datasource config file")
	private String sourceShardingConfig;

	@Option(names = { "--target-sharding-config" }, description = "Target sharding datasource config file")
	private String targetShardingConfig;

	@Option(names = { "--export-query" }, description = "Export query for source database")
	private String exportQuery;

	@Option(names = { "--target-table" }, description = "Target table name")
	private String targetTable;

	@Option(names = { "--target-columns" }, description = "Target columns name(Seperated by comma)")
	private String targetColumns;

	@Option(names = { "--target-delete-query" }, description = "Target delete query")
	private String targetDeleteQuery;

	@Option(names = { "--partition-key" }, description = "Partition source key from groupping")
	private String partitionKey;

	@Option(names = { "--batch-conut" }, description = "Insert batch count")
	private Integer batchCount;

	@Option(names = { "--insert-ignore" }, description = "Insert ignore mode")
	private Boolean insertIgnore;

	@Option(names = { "--upsert" }, description = "Upsert mode")
	private Boolean upsert;

	private boolean isExporting = true;
	private boolean isMysqlSource = false;
	private boolean isMysqlTarget = false;
	private BlockingQueue<Map<String, String>>[] queues;
	private boolean[] threadRunning;

	public static void main(String[] args) throws ClassNotFoundException {
		Main loadSphere = new Main();
		Integer exitCode = new CommandLine(loadSphere).execute(args);
		if (exitCode == 0) {
			try {
				loadSphere.migrationStart();
			} catch (Exception e) {
				System.out.println(e);
				System.exit(1);
			}
		}
	}

	@Override
	public Integer call() {
		try {
			File file = null;

			// =============================
			// Unmarshal config file
			// =============================
			file = new File(configFile);
			if (!file.exists()) {
				System.out.println("Config file not exists");
				return 1;
			}

			CONFIG = Config.unmarshal(file);

			if (workers != null) {
				CONFIG.setWorkers(workers);
			}

			// ===============================
			// Unmarshal source sharing config
			// ===============================
			if (sourceShardingConfig != null) {
				file = new File(sourceShardingConfig);
				if (!file.exists()) {
					System.out.println("Source config file not exists");
					return 1;
				}

				DataSource ds = YamlShardingDataSourceFactory.createDataSource(file);
				System.out.println("Set source datasource from " + sourceShardingConfig);
				CONFIG.setTargetDS(ds);
			}

			if (targetShardingConfig != null) {
				file = new File(targetShardingConfig);
				if (!file.exists()) {
					System.out.println("Target config file not exists");
					return 1;
				}

				DataSource ds = YamlShardingDataSourceFactory.createDataSource(file);
				System.out.println("Set target datasource from " + targetShardingConfig);
				CONFIG.setTargetDS(ds);
				if (CONFIG.isUpsert()) {
					System.out.println("Shardingsphere doesn't support upsert query, set upsert false");
					CONFIG.setUpsert(false);
				}
			}

			if (exportQuery != null) {
				CONFIG.setExportQuery(exportQuery);
			}

			TargetTable customTargetTable;
			boolean useCustomTargetTable = false;
			try {
				customTargetTable = CONFIG.getTargetTables()[0];
			} catch (Exception e) {
				customTargetTable = new TargetTable();
			}

			if (targetTable != null) {
				customTargetTable.setName(targetTable);
				useCustomTargetTable = true;
			}

			if (targetDeleteQuery != null) {
				customTargetTable.setDeleteQuery(targetDeleteQuery);
				useCustomTargetTable = true;
			}

			if (targetColumns != null) {
				String[] columns = targetColumns.replaceAll("\\s", "").toLowerCase().split(",");
				customTargetTable.setColumns(columns);
				useCustomTargetTable = true;
			}

			if (partitionKey != null) {
				CONFIG.setPartitionKey(partitionKey);
			}
			
			if(CONFIG.getPartitionKey() != null) {
				CONFIG.setPartitionKey(CONFIG.getPartitionKey().toLowerCase().trim());
			}

			if (batchCount != null) {
				CONFIG.setBatchCount(batchCount);
			}

			if (insertIgnore != null) {
				CONFIG.setInsertIgnore(insertIgnore);
			}

			if (upsert != null) {
				CONFIG.setUpsert(upsert);
			}

			if (useCustomTargetTable) {
				TargetTable[] targetTables = { customTargetTable };
				CONFIG.setTargetTables(targetTables);
			}

			// ============================
			// ID Generator handler
			// ============================
			IDGenerator idGenerator = CONFIG.getIdGenerator();
			if (idGenerator == null) {
				System.out.println("ID Generator is not defined, do nothing");
				idGenerator = new IDGenerator();
				CONFIG.setIdGenerator(idGenerator);
			}

			if (idGenerator.getClassName() != null) {

				System.out.println("Initialize class: " + idGenerator.getClassName());

				if (idGenerator.getColumnName() == null) {
					System.out.println(">> Column name not defined");
					return 1;
				}

				if (idGenerator.getParams() == null || idGenerator.getParams().length == 0) {
					System.out.println(">> Params not defined");
					return 1;
				}

				Class<?> c = Class.forName(idGenerator.getClassName());
				IDGeneratorHandler handler = (IDGeneratorHandler) c.newInstance();
				idGenerator.setIdGeneratorHandler(handler);

			}

			// ============================
			// Row modifier handler
			// ============================
			RowModifier rowModifier = CONFIG.getRowModifier();
			if (rowModifier == null) {
				System.out.println("Row Modifier is not defined, do nothing");
				rowModifier = new RowModifier();
				CONFIG.setRowModifier(rowModifier);
			}

			if (rowModifier.getClassName() != null) {
				System.out.println("Initialize class: " + rowModifier.getClassName());
				Class<?> c = Class.forName(rowModifier.getClassName());
				RowModifierHandler handler = (RowModifierHandler) c.newInstance();
				rowModifier.setRowModifierHandler(handler);
			}

			// ============================
			// Data load threads
			// ============================
			System.out.println(">> Load thread counts " + CONFIG.getWorkers());
			queues = new BlockingQueue[CONFIG.getWorkers()];
			threadRunning = new boolean[CONFIG.getWorkers()];
			for (int i = 0; i < CONFIG.getWorkers(); i++) {
				final int threadNumber = i;
				final BlockingQueue<Map<String, String>> queue = new ArrayBlockingQueue<Map<String, String>>(1000);

				// ========================
				// initialize queue
				// ========================
				queues[threadNumber] = queue;
				threadRunning[threadNumber] = true;

				// ============================
				// Start thread
				// ============================
				new Thread(new Runnable() {
					public void run() {
						List<Map<String, String>> entries = null;
						while (true) {

							Map<String, String> entry = queue.poll();

							// finish or wait 50ms
							if (!isExporting && entry == null) {
								break;
							} else if (entry == null) {
								sleep(50);
								continue;
							}

							if (entries == null) {
								entries = new ArrayList<Map<String, String>>();
							}

							// ID generate
							idGenerate(entry);

							// modify row values
							modifyRow(entry);

							// add entry array (single batch insert)
							entries.add(entry);

							if (entries.size() >= CONFIG.getBatchCount()) {
								insertRows(entries);
								entries = null;
							}
						}

						// last insert
						if (entries != null) {
							insertRows(entries);
						}

						// set running thread flag false
						threadRunning[threadNumber] = false;
					}
				}).start();
			}
		} catch (Exception e) {
			System.out.println(e);
			System.exit(1);
		}
		return 0;
	}

	// ============================
	// Enqueue
	// ============================
	private void enqueue(final Map<String, String> entries, final int slot) {
		while (true) {
			try {
				queues[slot].add(entries);
				break;
			} catch (Exception e) {
				Main.sleep(10);
			}
		}
	}

	private void startMonitorThread() {
		new Thread(new Runnable() {

			public void run() {
				while (true) {
					if (isExporting || isWorking()) {
						for (TargetTable targetTable : CONFIG.getTargetTables()) {
							System.out.println(String.format("%10s: %10d inserted", targetTable.getName(),
									targetTable.getInsertedRows().get()));
						}
						sleep(1000);
						continue;
					}
					for (TargetTable targetTable : CONFIG.getTargetTables()) {
						System.out.println(String.format("%10s: %10d inserted.FIN", targetTable.getName(),
								targetTable.getInsertedRows().get()));
					}
					System.exit(0);
				}
			}
		}).start();
	}

	private boolean isMysql(DataSource ds) throws SQLException {
		Connection connection = ds.getConnection();
		try {
			connection.createStatement().executeQuery("show variables like '%innodb%'");
		} catch (Exception e) {
			return false;
		}
		connection.close();
		return true;
	}

	private void migrationStart() throws SQLException {

		// ============================
		// Create connection pool
		// ============================
		Connection sourceConn = CONFIG.getSourceDS().getConnection();
		Connection targetConn = CONFIG.getTargetDS().getConnection();
		isMysqlSource = isMysql(CONFIG.getSourceDS());
		isMysqlTarget = isMysql(CONFIG.getTargetDS());

		// ============================
		// Get rows from source
		// ============================
		Statement sourceStmt = sourceConn.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
				java.sql.ResultSet.CONCUR_READ_ONLY);
		if (isMysqlSource) {
			System.out.println("Source is mysql, set fetch size to Integer.MIN_VALUE");
			sourceStmt.setFetchSize(Integer.MIN_VALUE);
		} else {
			System.out.println("Source is not mysql, set fetch size to 5000");
			sourceStmt.setFetchSize(5000);
		}

		if (!isMysqlTarget) {
			System.out.println("Target is not mysql, set insert ignore false and upsert mode false");
			CONFIG.setInsertIgnore(false);
			CONFIG.setUpsert(false);
		}

		// ============================
		// Delete rows (target)
		// ============================
		for (TargetTable targetTable : CONFIG.getTargetTables()) {
			targetTable.delete(targetConn);
		}
		targetConn.close();

		System.out.println("> Get result set start");
		System.out.println("> Query\n"+CONFIG.getExportQuery());
		ResultSet rs = sourceStmt.executeQuery(CONFIG.getExportQuery());

		// ============================
		// Generate pstmt query
		// ============================
		ResultSetMetaData rsMeta = rs.getMetaData();
		int records = rsMeta.getColumnCount();

		// Config target tables
		for (TargetTable targetTable : CONFIG.getTargetTables()) {

			// target is all columns
			if (targetTable.getColumns() == null) {
				String[] columns = new String[records];
				for (int i = 0; i < columns.length; i++) {
					columns[i] = rsMeta.getColumnLabel(i + 1).toLowerCase();
				}
				targetTable.setColumns(columns);
			}

			// column rename mapper
			Map<String, String> map = new HashMap<String, String>();
			for (String column : targetTable.getColumns()) {
				String[] arr = column.toLowerCase().split(":");
				String sColumnName = arr[0];
				String tColumnName = arr.length > 1 ? arr[1] : arr[0];
				map.put(sColumnName, tColumnName);
			}

			// ID generator mapper
			if (CONFIG.getIdGenerator().getColumnName() != null) {
				String genColumnName = CONFIG.getIdGenerator().getColumnName().toLowerCase();
				map.put(genColumnName, genColumnName);
			}

			String cols = "", vals = "", upds = "";
			for (Entry<String, String> entry : map.entrySet()) {
				cols += String.format(",%s", entry.getValue());
				vals += ",?";
				upds += String.format(",%s=values(%s)", entry.getValue(), entry.getValue());
				targetTable.getInsertColumns().add(entry.getKey());
			}

			// Binding columns
			String bindingCols = "(" + vals.replaceFirst(",", "") + ")";
			targetTable.setBindingCols(bindingCols);

			// Insert query
			String insertQuery = "";
			insertQuery += "insert " + (CONFIG.isInsertIgnore() ? "ignore " : "") + "into " + targetTable.getName();
			insertQuery += "(" + cols.replaceFirst(",", "") + ")";
			insertQuery += "values";
			insertQuery += bindingCols;
			targetTable.setInsertQuery(insertQuery);

			// Upsert params
			String upsertParam = " on duplicate key update " + upds.replaceFirst(",", "");
			targetTable.setUpsertParam(upsertParam);

		}

		// ============================
		// Start monitoring thread
		// ============================
		startMonitorThread();

		// ============================
		// Enqueue data
		// ============================
		while (rs.next()) {
			Map<String, String> entry = new HashMap<String, String>();
			for (int i = 1; i <= records; i++) {
				entry.put(rsMeta.getColumnLabel(i).toLowerCase(), rs.getString(i));
			}

			int slot;
			if (entry.containsKey(CONFIG.getPartitionKey())) {
				slot = getSlot(entry.get(CONFIG.getPartitionKey()));
			} else {
				slot = RAND.nextInt(CONFIG.getWorkers());
			}
			enqueue(entry, slot);

		}
		rs.close();
		sourceConn.close();
		isExporting = false;
	}

	// ================================
	// Generate new ID for target table
	// ================================
	private void idGenerate(final Map<String, String> entry) {
		int execCount = 0;
		while (execCount < CONFIG.getRetryCount()) {
			try {
				execCount++;
				if (CONFIG.getIdGenerator().getIdGeneratorHandler() != null) {
					String[] paramCols = CONFIG.getIdGenerator().getParams();
					String[] paramVals = new String[paramCols.length];
					for (int i = 0; i < paramCols.length; i++) {
						paramVals[i] = entry.get(paramCols[i]);
					}
					String idGenValue = CONFIG.getIdGenerator().getIdGeneratorHandler().generate(paramVals);
					entry.put(CONFIG.getIdGenerator().getColumnName(), idGenValue);
				}
				return;
			} catch (Exception e) {
				System.out.println(e);
				sleep(CONFIG.getRetryMili());
			}

		}
		System.out.println("idGenerate - Retry count(" + CONFIG.getRetryCount() + ") has been exceeded, exit");
		System.exit(1);
	}

	// ================================
	// Modify row
	// ================================
	private void modifyRow(final Map<String, String> entry) {
		int execCount = 0;
		while (execCount < CONFIG.getRetryCount()) {
			try {
				execCount++;
				if (CONFIG.getRowModifier().getRowModifierHandler() != null) {
					CONFIG.getRowModifier().getRowModifierHandler().doModify(entry);
				}
				return;
			} catch (Exception e) {
				System.out.println(e);
				sleep(CONFIG.getRetryMili());
			}
		}
		System.out.println("modifyRow - Retry count(" + CONFIG.getRetryCount() + ") has been exceeded, exit");
		System.exit(1);
	}

	// ================================
	// Insert single row
	// ================================
	private void insertRows(final List<Map<String, String>> entries) {
		Connection conn = null;
		int execCount = 0;

		// Insert database
		while (execCount < CONFIG.getRetryCount()) {

			PreparedStatement pstmt = null;
			try {
				execCount++;

				conn = CONFIG.getTargetDS().getConnection();
				conn.setAutoCommit(false);
				for (TargetTable targetTable : CONFIG.getTargetTables()) {
					StringBuffer sbInsert = new StringBuffer();
					sbInsert.append(targetTable.getInsertQuery());

					if (isMysqlTarget) {

						// extended-insert
						for (int i = 0; i < entries.size() - 1; i++) {
							sbInsert.append(",").append(targetTable.getBindingCols());
						}
						if (CONFIG.isUpsert()) {
							sbInsert.append(targetTable.getUpsertParam());
						}

						int pos = 1;
						pstmt = conn.prepareStatement(sbInsert.toString());
						for (Map<String, String> entry : entries) {
							for (String column : targetTable.getInsertColumns()) {
								pstmt.setString(pos++, entry.get(column));
							}
						}
						pstmt.execute();
						pstmt.close();
					} else {
						// batch insert
						pstmt = conn.prepareStatement(sbInsert.toString());
						for (Map<String, String> entry : entries) {
							int pos = 1;
							for (String column : targetTable.getInsertColumns()) {
								pstmt.setString(pos++, entry.get(column));
							}
							pstmt.addBatch();
							pstmt.clearParameters();
						}
						pstmt.executeBatch();
						pstmt.close();
					}
					targetTable.getInsertedRows().addAndGet(entries.size());

				}
				conn.commit();
				conn.setAutoCommit(true);
				return;
			} catch (Exception e) {
				System.out.println(e);
				try {
					conn.rollback();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
				sleep(CONFIG.getRetryMili());
			} finally {
				try {
					conn.close();
				} catch (Exception e) {
				}
			}
		}
		System.out.println("Retry count(" + CONFIG.getRetryCount() + ") has been exceeded, exit");
		System.exit(1);
	}

	public static int getBatchedCount(int[] cnts) {
		int r = 0;
		for (int i : cnts) {
			if (i > 0) {
				r++;
			}
		}
		return r;
	}

	public static void sleep(long ms) {
		try {
			Thread.sleep(ms);
		} catch (InterruptedException e) {
		}
	}

	public int getSlot(final String s) {
		Checksum checksum = new CRC32();
		try {
			byte[] bytes = s.getBytes();
			checksum.update(bytes, 0, bytes.length);
			return (int) (checksum.getValue() % CONFIG.getWorkers());
		} catch (Exception e) {
		}
		return 0;
	}

	public boolean isWorking() {
		for (boolean b : threadRunning) {
			if (b) {
				return true;
			}
		}
		return false;
	}
}
