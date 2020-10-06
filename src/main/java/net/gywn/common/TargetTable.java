package net.gywn.common;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import net.gywn.Main;

@Getter
@Setter
@ToString
public class TargetTable {
	private String name;
	private String[] columns;
	private String deleteQuery;
	private String insertQuery;
	private String upsertParam;
	private final List<String> insertColumns = new ArrayList<String>();
	private AtomicLong insertedRows = new AtomicLong();
	private QueryType queryType = QueryType.INSERT;

	public enum QueryType {
		INSERT {

			@Override
			public String getQuery(TargetTable targetTable) {
				return targetTable.getInsertQuery();
			}

			@Override
			public List<String> getColumns(TargetTable targetTable) {
				// TODO Auto-generated method stub
				return targetTable.getInsertColumns();
			}
		},
		UPSERT {

			@Override
			public String getQuery(TargetTable targetTable) {
				return String.format("%s %s", targetTable.getInsertQuery(), targetTable.getUpsertParam());
			}

			@Override
			public List<String> getColumns(TargetTable targetTable) {
				// TODO Auto-generated method stub
				return targetTable.getInsertColumns();
			}
		};

		public abstract String getQuery(final TargetTable targetTable);

		public abstract List<String> getColumns(final TargetTable targetTable);
	}

	public void delete(final Connection connection) throws SQLException {
		if (deleteQuery == null || deleteQuery.trim().length() == 0) {
			System.out.println("delete query for [" + name + "] is not defined, skip!");
			return;
		}
		long deleteRows = 0;
		long lastTimeMillis = System.currentTimeMillis();
		System.out.println("Start delete : " + name);
		System.out.println(">> " + deleteQuery);
		PreparedStatement pstmt = connection.prepareStatement(deleteQuery);
		while (true) {
			int affectedRows = 0;
			try {
				affectedRows += pstmt.executeUpdate();
			} catch (Exception e) {
				System.out.println(">>"+e);
				Main.sleep(1000);
				continue;
			}

			// Print delete status
			long currentTimeMillis = System.currentTimeMillis();
			if (currentTimeMillis - lastTimeMillis > 5000) {
				lastTimeMillis = currentTimeMillis;
				System.out.println(">>>> Deleting.. " + deleteRows);
			}

			if (affectedRows == 0) {
				System.out.println("Delete process finisehd");
				break;
			}
		}
		System.out.println(">> " + deleteRows + " rows deleted");
		pstmt.close();
	}
}