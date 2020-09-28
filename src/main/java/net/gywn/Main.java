package net.gywn;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import io.shardingsphere.shardingjdbc.api.yaml.YamlShardingDataSourceFactory;
import net.gywn.common.IDGenerator;
import net.gywn.common.TargetTable;
import net.gywn.common.TargetTable.QueryType;
import net.gywn.algorithm.IDGeneratorHandler;
import net.gywn.common.Config;
import picocli.CommandLine;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

import static picocli.CommandLine.*;

public class Main implements Callable<Integer> {

	public static Config CONFIG;

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

	private final BlockingQueue<Map<String, String>> queue = new ArrayBlockingQueue<Map<String, String>>(1000);

	public static void main(String[] args) {
		Main loadSphere = new Main();
		Integer exitCode = new CommandLine(loadSphere).execute(args);
		if (exitCode == 0) {
			try {
				loadSphere.migrationStart();
			}catch(Exception e) {
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

				IDGeneratorHandler handler = (IDGeneratorHandler) (Class.forName(idGenerator.getClassName())
						.newInstance());
				idGenerator.setIdGeneratorHandler(handler);

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
								Map<String, String> entry = queue.take();
								generateID(entry);
								insertRows(entry);
							} catch (Exception e) {
								System.out.println(e);
								System.exit(1);
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
	private void enqueue(final Map<String, String> entry) {
		while (true) {
			try {
				queue.add(entry);
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
					if (exporting || !queue.isEmpty()) {
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

	private void migrationStart() throws SQLException {
		// ============================
		// Delete rows
		// ============================
		Connection connection = null;
		connection = CONFIG.getTargetDS().getConnection();
		for (TargetTable targetTable : CONFIG.getTargetTables()) {
			targetTable.delete(connection);
		}
		connection.close();

		// ============================
		// Get rows from source
		// ============================
		connection = CONFIG.getSourceDS().getConnection();
		Statement statement = connection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
				java.sql.ResultSet.CONCUR_READ_ONLY);
		statement.setFetchSize(1000);

		System.out.println("> Get result set start");
		ResultSet rs = statement.executeQuery(CONFIG.getExportQuery());

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
				targetTable.getUpsertColumns().add(entry.getKey());
			}

			String query = "";
			query += "insert into " + targetTable.getName();
			query += "(" + cols.replaceFirst(",", "") + ")";
			query += "values";
			query += "(" + vals.replaceFirst(",", "") + ")";
			targetTable.setInsertQuery(query);

			query += " on duplicate key update " + upds.replaceFirst(",", "");
			targetTable.setUpsertQuery(query);

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
		while (rs.next()) {
			Map<String, String> entry = new HashMap<String, String>();
			for (int i = 1; i <= records; i++) {
				entry.put(rsMeta.getColumnLabel(i).toLowerCase(), rs.getString(i));
			}
			enqueue(entry);
		}
		rs.close();
		connection.close();
		exporting = false;
	}

	// ================================
	// Generate new ID for target table
	// ================================
	private void generateID(final Map<String, String> entry) {

		if (CONFIG.getIdGenerator().getIdGeneratorHandler() != null) {
			while (true) {
				try {
					String[] paramCols = CONFIG.getIdGenerator().getParams();
					String[] paramVals = new String[paramCols.length];
					for (int i = 0; i < paramCols.length; i++) {
						paramVals[i] = entry.get(paramCols[i]);
					}
					String genValue = CONFIG.getIdGenerator().getIdGeneratorHandler().generate(paramVals);
					entry.put(CONFIG.getIdGenerator().getColumnName(), genValue);
					break;
				} catch (Exception e) {
					e.printStackTrace();
					sleep(1000);
				}
			}
		}
	}

	// ================================
	// Insert single row
	// ================================
	private void insertRows(final Map<String, String> entry) {
		Connection conn = null;

		while (true) {
			PreparedStatement pstmt = null;
			try {
				conn = CONFIG.getTargetDS().getConnection();
				for (TargetTable targetTable : CONFIG.getTargetTables()) {
					QueryType queryType = targetTable.getQueryType();
					pstmt = conn.prepareStatement(queryType.getQuery(targetTable));
					int pos = 1;
					for (String column : queryType.getColumns(targetTable)) {
						pstmt.setString(pos++, entry.get(column));
					}
					targetTable.getInsertedRows().addAndGet((pstmt.executeUpdate() + 1) / 2);
					pstmt.close();
				}
				break;
			} catch (Exception e) {
				System.out.println(e);
				System.out.println("[ERROR] " + entry);
				sleep(1000);
			} finally {
				try {
					conn.close();
				} catch (Exception e) {
				}
			}
		}
	}

	public static void sleep(long ms) {
		try {
			Thread.sleep(ms);
		} catch (InterruptedException e) {
		}
	}
}
