package net.gywn.algorithm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.shardingsphere.api.sharding.standard.PreciseShardingAlgorithm;
import org.apache.shardingsphere.api.sharding.standard.PreciseShardingValue;

//import io.shardingsphere.api.algorithm.sharding.PreciseShardingValue;
//import io.shardingsphere.api.algorithm.sharding.standard.PreciseShardingAlgorithm;

public class PreciseShardingCRC32 implements PreciseShardingAlgorithm<Comparable<?>> {

	public String doSharding(Collection<String> availableTargetNames,
			PreciseShardingValue<Comparable<?>> shardingValue) {
		ArrayList<String> list = new ArrayList<String>(availableTargetNames);
		Checksum checksum = new CRC32();
		try {
			byte[] bytes = shardingValue.getValue().toString().getBytes();
			checksum.update(bytes, 0, bytes.length);
			int seq = (int) (checksum.getValue() % list.size());
			return list.get(seq);
		} catch (Exception e) {
		}
		return list.get(0);
	}
}