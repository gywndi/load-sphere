package net.gywn.algorithm;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.shardingsphere.api.sharding.standard.PreciseShardingAlgorithm;
import org.apache.shardingsphere.api.sharding.standard.PreciseShardingValue;

public class PreciseShardingJavaHash implements PreciseShardingAlgorithm<Comparable> {

	public String doSharding(Collection<String> availableTargetNames, PreciseShardingValue<Comparable> shardingValue) {
		ArrayList<String> list = new ArrayList<String>(availableTargetNames);
		try {
			int number = Math.abs(shardingValue.getValue().toString().hashCode());
			int seq = (int) (number % list.size());
			return list.get(seq);
		} catch (Exception e) {
		}
		return list.get(0);
	}
}