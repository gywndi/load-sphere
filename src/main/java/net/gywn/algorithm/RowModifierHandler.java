package net.gywn.algorithm;

import java.util.Map;

public interface RowModifierHandler {
	public void doModify(final Map<String, String> map) throws Exception;
}
