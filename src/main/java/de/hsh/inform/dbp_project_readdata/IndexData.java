package de.hsh.inform.dbp_project_readdata;

import java.util.HashMap;

import redis.clients.jedis.Jedis;

public class IndexData {
	HashMap<String, Double> map = new HashMap<String, Double>();

	public boolean isEmpty() {
		return this.map.isEmpty();
	}

	public static String appendZeros(String s, Integer a) {
		String zeroString = "";
		for (int i = 0; i < a; i++) {
			zeroString = "0" + zeroString;
		}
		return zeroString + s;
	}

	public static String convertIpAdress(String s) {
		if(s.contains(".")){
			s = s.replaceAll("[^\\w.]+", "");
			String[] address = s.split("\\.");
	
			for (int i = 0; i < 4; i++) {
				if (i > 0) {
					if (address[i].length() == 1) {
						address[i] = appendZeros(address[i], 2);
					} else if (address[i].length() == 2) {
						address[i] = appendZeros(address[i], 1);
					}
				}
			}
			return String.join("", address);
		}else{
			return s;
		}
	}

	public static void setIndexes(Jedis jedis, AddInformation data, long cnt) {
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
			String adr = data.metaData.get(App.sourceAddr).replaceAll("[^\\w.]+", "");
			sourceAddrIndex.map.put(indexSetSourceAddr + convertIpAdress(adr), Double.valueOf(convertIpAdress(adr)));
		}

		// destinationAddr
		if (data.metaData.containsKey(App.destinationAddr)) {
			String adr = data.metaData.get(App.destinationAddr).replaceAll("[^\\w.]+", "");
			destinationAddrIndex.map.put(indexSetDestAddr + convertIpAdress(adr), Double.valueOf(convertIpAdress(adr)));
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
			jedis.zadd("index:" + dataContains_String, containsByteSequenceIndex.map);
			jedis.lpush("indexSets:" + dataContains_String + ":" + data.dataData.get(dataContains_String), cnt + "");
		}

		if (!timeStampIndex.isEmpty()) {
			jedis.zadd("index:timestamp", timeStampIndex.map);
			jedis.lpush(indexSetTimestamp + data.metaData.get("timestamp"), cnt + "");
		}

		if (!sourceAddrIndex.isEmpty()) {
			jedis.zadd("index:" + App.sourceAddr, sourceAddrIndex.map);
			jedis.lpush(
					indexSetSourceAddr + convertIpAdress(data.metaData.get(App.sourceAddr).replaceAll("[^\\w.]+", "")),
					cnt + "");
		}

		if (!destinationAddrIndex.isEmpty()) {
			jedis.zadd("index:" + App.destinationAddr, destinationAddrIndex.map);
			jedis.lpush(indexSetDestAddr + convertIpAdress(data.metaData.get(App.destinationAddr).replaceAll("[^\\w.]+", "")),
					cnt + "");
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
