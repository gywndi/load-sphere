package net.gywn.common;

import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import net.gywn.algorithm.IDGeneratorHandler;

@Setter
@Getter
@ToString
public class IDGenerator {
	private String className;
	private String[] params;
	private String columnName;
	private IDGeneratorHandler idGeneratorHandler;
}
