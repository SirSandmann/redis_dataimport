package de.hsh.inform.dbp_project_readdata;

import java.util.HashMap;

import redis.clients.jedis.Jedis;

public class IndexData {
	HashMap<String, Double> map = new HashMap<String, Double>();

	public boolean isEmpty() {
		return this.map.isEmpty();
	}

	public static void setIndexes(Jedis jedis, AddInformation data, int cnt) {
		IndexData timeStampIndex = new IndexData();
		IndexData containsByteSequenceIndex = new IndexData();
		IndexData sourceAddrIndex = new IndexData();
		IndexData destinationAddrIndex = new IndexData();
		IndexData sourcePortIndex = new IndexData();
		IndexData destinationPortIndex = new IndexData();

		final String dataContains_String = "dataContains_0x350xAF0xF8";

		final String indexSetTimestamp = "indexSets:timestamp:";
		final String indexSetSourceAddr = "indexSets:sourceAddr:";
		final String indexSetDestAddr = "indexSets:destinationAddr:";
		final String indexSetSourcePort = "indexSets:sourcePort:";
		final String indexSetDestPort = "indexSets:destinationPort:";

		/* TODO: put in Array and loop through */
		// indexTimeStamp
		if (data.metaData.containsKey("timestamp")) {
			timeStampIndex.map.put(indexSetTimestamp + data.metaData.get("timestamp"),
					Double.valueOf(data.metaData.get("timestamp")));
		}

		// sourceAddr
		if (data.metaData.containsKey(App.sourceAddr)) {
			sourceAddrIndex.map.put(indexSetSourceAddr + data.metaData.get(App.sourceAddr).replaceAll("\\D+", ""),
					Double.valueOf(data.metaData.get(App.sourceAddr).replaceAll("\\D+", "")));
		}

		// destinationAddr
		if (data.metaData.containsKey(App.destinationAddr)) {
			destinationAddrIndex.map.put(
					indexSetDestAddr + data.metaData.get(App.destinationAddr).replaceAll("\\D+", ""),
					Double.valueOf(data.metaData.get(App.destinationAddr).replaceAll("\\D+", "")));
		}

		// sourcePort
		if (data.metaData.containsKey(App.sourcePort)) {
			sourcePortIndex.map.put(indexSetSourcePort + data.metaData.get(App.sourcePort).replaceAll("\\D+", ""),
					Double.valueOf(data.metaData.get(App.sourcePort).replaceAll("\\D+", "")));
		}

		// destinationPort
		if (data.metaData.containsKey(App.destinationPort)) {
			destinationPortIndex.map.put(
					indexSetDestPort + data.metaData.get(App.destinationPort).replaceAll("\\D+", ""),
					Double.valueOf(data.metaData.get(App.destinationPort).replaceAll("\\D+", "")));
		}

		// ----------------------- DATADATA------------------------------
		if (data.dataData.containsKey(dataContains_String)) {
			if (Boolean.valueOf(data.dataData.get(dataContains_String))) {
				containsByteSequenceIndex.map
						.put("indexSets:" + dataContains_String + ":" + data.dataData.get(dataContains_String), 1.0);
			} else {
				containsByteSequenceIndex.map
						.put("indexSets:" + dataContains_String + ":" + data.dataData.get(dataContains_String), 0.0);
			}
		}

		// Add Sets an Indexes
		if (!containsByteSequenceIndex.isEmpty()) {
			jedis.zadd("index:" + dataContains_String + data.dataData.get(dataContains_String),
					containsByteSequenceIndex.map);
			jedis.lpush("indexSets:" + dataContains_String + ":" + data.dataData.get(dataContains_String), cnt + "");
		}

		if (!timeStampIndex.isEmpty()) {
			jedis.zadd("index:timestamp", timeStampIndex.map);
			jedis.lpush(indexSetTimestamp + data.metaData.get("timestamp"), cnt + "");
		}

		if (!sourceAddrIndex.isEmpty()) {
			jedis.zadd("index:" + App.sourceAddr, sourceAddrIndex.map);
			jedis.lpush(indexSetSourceAddr + data.metaData.get(App.sourceAddr).replaceAll("\\D+", ""), cnt + "");
		}

		if (!destinationAddrIndex.isEmpty()) {
			jedis.zadd("index:" + App.destinationPort, destinationAddrIndex.map);
			jedis.lpush(indexSetDestAddr + data.metaData.get(App.destinationAddr).replaceAll("\\D+", ""), cnt + "");
		}

		if (!sourcePortIndex.isEmpty()) {
			jedis.zadd("index:" + App.sourcePort, sourcePortIndex.map);
			jedis.lpush(indexSetSourcePort + data.metaData.get(App.sourcePort).replaceAll("\\D+", ""), cnt + "");
		}

		if (!sourcePortIndex.isEmpty()) {
			jedis.zadd("index:" + App.destinationPort, destinationPortIndex.map);
			jedis.lpush(indexSetDestPort + data.metaData.get(App.destinationPort).replaceAll("\\D+", ""), cnt + "");
		}
	}
}
