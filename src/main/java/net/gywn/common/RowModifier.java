package net.gywn.common;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import net.gywn.algorithm.IDGeneratorHandler;
import net.gywn.algorithm.RowModifierHandler;
import net.gywn.algorithm.RowModifierHandlerImpl;

@Setter
@Getter
@ToString
public class RowModifier {
	private String className;
	private String[] params;
	private String columnName;
	private RowModifierHandler rowModifierHandler = new RowModifierHandlerImpl();
	private Map<String, String> properties = new HashMap<String, String>();
}
