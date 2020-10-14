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

//import io.shardingsphere.shardingjdbc.api.yaml.YamlShardingDataSourceFactory;
import net.gywn.common.IDGenerator;
import net.gywn.common.RowModifier;
import net.gywn.common.TargetTable;
import net.gywn.common.TargetTable.QueryType;
import net.gywn.algorithm.IDGeneratorHandler;
import net.gywn.algorithm.RowModifierHandler;
import net.gywn.common.Config;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import static picocli.CommandLine.*;

public class Main implements Callable<Integer> {

	public static Config CONFIG;
	public static AtomicInteger currentWorkers = new AtomicInteger();

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

	public boolean exporting = true;

	private final BlockingQueue<List<Map<String, String>>> queue = new ArrayBlockingQueue<List<Map<String, String>>>(
			1000);

	public static void main(String[] args) throws ClassNotFoundException {
		Main loadSphere = new Main();
//		Integer exitCode = new CommandLine(loadSphere).execute(args);
		Integer exitCode = new CommandLine(loadSphere).execute(new String[] { "--config-file", "config-mysql.yml",
				"--target-sharding-config", "config-sharding.yml" });
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
			// Start load threads
			// ============================
			System.out.println(">> Load thread counts " + CONFIG.getWorkers());
			for (int i = 0; i < CONFIG.getWorkers(); i++) {
				new Thread(new Runnable() {
					public void run() {
						while (true) {
							try {
								List<Map<String, String>> entries = queue.take();
								currentWorkers.incrementAndGet();

								// massage rows (ID generate / Row modify)
								for (Map<String, String> entry : entries) {
									idGenerate(entry);
									modifyRow(entry);
								}

								// insert target database
								insertRows(entries);
							} catch (Exception e) {
								System.out.println(e);
								System.exit(1);
							} finally {
								currentWorkers.decrementAndGet();
							}
						}
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
	private void enqueue(final List<Map<String, String>> entries) {
		while (true) {
			try {
				queue.add(entries);
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
					if (exporting || !queue.isEmpty() || currentWorkers.get() > 0) {
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
		// Check target database type
		// ============================
		if (!isMysql(CONFIG.getTargetDS())) {
			System.out.println("Target is not mysql, set insertIgnore false");
			CONFIG.setInsertIgnore(false);
		}

		// ============================
		// Delete rows
		// ============================
		Connection targetConn = CONFIG.getTargetDS().getConnection();
		for (TargetTable targetTable : CONFIG.getTargetTables()) {
			targetTable.delete(targetConn);
		}
		targetConn.close();

		// ============================
		// Get rows from source
		// ============================
		Connection sourceConn = CONFIG.getSourceDS().getConnection();
		Statement sourceStmt = sourceConn.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
				java.sql.ResultSet.CONCUR_READ_ONLY);
		if (isMysql(CONFIG.getSourceDS())) {
			System.out.println("Source is mysql, set fetch size to Integer.MIN_VALUE");
			sourceStmt.setFetchSize(Integer.MIN_VALUE);
		} else {
			System.out.println("Source is not mysql, set fetch size to 5000");
			sourceStmt.setFetchSize(5000);
		}

		System.out.println("> Get result set start");
		ResultSet rs = sourceStmt.executeQuery(CONFIG.getExportQuery());

		// ============================
		// Generate pstmt query
		// ============================
		ResultSetMetaData rsMeta = rs.getMetaData();
		int records = rsMeta.getColumnCount();

		// ============================
		// Config target tables
		// ============================
		for (TargetTable targetTable : CONFIG.getTargetTables()) {
			if (targetTable.getColumns() == null) {
				String[] columns = new String[records];
				for (int i = 0; i < columns.length; i++) {
					columns[i] = rsMeta.getColumnLabel(i + 1).toLowerCase();
				}
				targetTable.setColumns(columns);
			}

			Map<String, String> map = new HashMap<String, String>();
			for (String column : targetTable.getColumns()) {
				String[] arr = column.toLowerCase().split(":");
				String sColumnName = arr[0];
				String tColumnName = arr.length > 1 ? arr[1] : arr[0];
				map.put(sColumnName, tColumnName);
			}

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

			String insertQuery = "";
			insertQuery += "insert " + (CONFIG.isInsertIgnore() ? "ignore " : "") + "into " + targetTable.getName();
			insertQuery += "(" + cols.replaceFirst(",", "") + ")";
			insertQuery += "values";
			insertQuery += "(" + vals.replaceFirst(",", "") + ")";
			targetTable.setInsertQuery(insertQuery);

			String upsertParam = " on duplicate key update " + upds.replaceFirst(",", "");
			targetTable.setUpsertParam(upsertParam);

			if (CONFIG.isUpsert()) {
				targetTable.setQueryType(TargetTable.QueryType.UPSERT);
			}
		}

		// ============================
		// Start monitoring thread
		// ============================
		startMonitorThread();

		// ============================
		// Enqueue data
		// ============================
		List<Map<String, String>> entries = null;
		while (rs.next()) {
			if (entries == null) {
				entries = new ArrayList<Map<String, String>>();
			}
			Map<String, String> entry = new HashMap<String, String>();
			for (int i = 1; i <= records; i++) {
				entry.put(rsMeta.getColumnLabel(i).toLowerCase(), rs.getString(i));
			}
			entries.add(entry);
			if (entries.size() >= CONFIG.getInsertBatchCount()) {
				enqueue(entries);
				entries = null;
			}
		}
		if (entries != null) {
			enqueue(entries);
		}
		rs.close();
		sourceConn.close();
		exporting = false;
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
					QueryType queryType = targetTable.getQueryType();
					pstmt = conn.prepareStatement(queryType.getQuery(targetTable));
					for (Map<String, String> entry : entries) {
						int pos = 1;
						for (String column : queryType.getColumns(targetTable)) {
							pstmt.setString(pos++, entry.get(column));
						}
						pstmt.addBatch();
						pstmt.clearParameters();
					}
					pstmt.executeBatch();
					pstmt.close();
					targetTable.getInsertedRows().addAndGet(entries.size());
				}
				conn.commit();
				conn.setAutoCommit(true);
				return;
			} catch (Exception e) {
				System.out.println(e);
//				System.out.println("[ERROR] " + entry);
				try {
					conn.rollback();
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
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
}
