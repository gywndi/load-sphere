package net.gywn.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;

import javax.sql.DataSource;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class Config {

	private int workers = 8;
	private String sourceURL;
	private String sourceUsername;
	private String sourcePassword;
	private DataSource sourceDS;
	private DataSource targetDS;

	private String exportQuery;
	private IDGenerator idGenerator;
	private RowModifier rowModifier;
	private TargetTable[] targetTables;
	private boolean upsert = false;
	private boolean insertIgnore = false;
	private int retryCount = 10;
	private int retryMili = 5000;
	private int insertBatchCount = 30;

	public static Config unmarshal(final File yamlFile) throws IOException, ClassNotFoundException {
		Class.forName("com.mysql.jdbc.Driver");
		Class.forName("oracle.jdbc.driver.OracleDriver");
		try (FileInputStream fileInputStream = new FileInputStream(yamlFile);
				InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream, "UTF-8")) {
			return new Yaml(new Constructor(Config.class)).loadAs(inputStreamReader, Config.class);
		}
	}
}
