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

	private int workers;
	private String sourceURL;
	private String sourceUsername;
	private String sourcePassword;
	private DataSource sourceDS;
	private DataSource targetDS;

	private String exportQuery;
	private IDGenerator idGenerator;
	private TargetTable[] targetTables;
	private boolean upsert = false;

	public static Config unmarshal(final File yamlFile) throws IOException {
		try (FileInputStream fileInputStream = new FileInputStream(yamlFile);
				InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream, "UTF-8")) {
			return new Yaml(new Constructor(Config.class)).loadAs(inputStreamReader, Config.class);
		}
	}
}
