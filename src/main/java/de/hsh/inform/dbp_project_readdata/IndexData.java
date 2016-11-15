package de.hsh.inform.dbp_project_readdata;

import java.util.HashMap;

import redis.clients.jedis.Jedis;

public class IndexData {
	HashMap<String, Double> map = new HashMap<String, Double>();

	public boolean isEmpty() {
		return this.map.isEmpty();
	}
	
	public static void setIndexes(Jedis jedis, PackageData data) {
		IndexData timeStampIndex = new IndexData();
		IndexData containsByteSequenceIndex = new IndexData();
		IndexData sourceAddrIndex = new IndexData();
		IndexData destinationAddrIndex = new IndexData();
		IndexData sourcePortIndex = new IndexData();
		IndexData destinationPortIndex = new IndexData();
		
		/* TODO: put in Array and loop through*/
		// indexTimeStamp
		if(data.metaData.containsKey("timestamp")){
			timeStampIndex.map.put(data.metaData.get("timestamp"), Double.valueOf(data.metaData.get("timestamp")));
		}
		
		// sourceAddr
		if(data.metaData.containsKey(App.sourceAddr)){
			sourceAddrIndex.map.put(data.metaData.get(App.sourceAddr),
					Double.valueOf(data.metaData.get(App.sourceAddr).replaceAll("\\D+", "")));
		}

		// destinationAddr
		if(data.metaData.containsKey(App.destinationAddr)){
			destinationAddrIndex.map.put(data.metaData.get(App.destinationAddr),
					Double.valueOf(data.metaData.get(App.destinationAddr).replaceAll("\\D+", "")));
		}
		
		
		// sourcePort
		if(data.metaData.containsKey(App.sourcePort)){
			sourcePortIndex.map.put(data.metaData.get(App.sourcePort),
					Double.valueOf(data.metaData.get(App.sourcePort).replaceAll("\\D+", "")));
		}

		// destinationPort
		if(data.metaData.containsKey(App.destinationPort)){
			destinationPortIndex.map.put(data.metaData.get(App.destinationPort),
					Double.valueOf(data.metaData.get(App.destinationPort).replaceAll("\\D+", "")));
		}
		
		// ----------------------- DATADATA------------------------------
		if(data.dataData.containsKey("dataContains_0x350xAF0xF8")){
			if(Boolean.valueOf(data.dataData.get("dataContains_0x350xAF0xF8"))){
				containsByteSequenceIndex.map.put(data.dataData.get("dataContains_0x350xAF0xF8"), 1.0 );
			}else{
				containsByteSequenceIndex.map.put(data.dataData.get("dataContains_0x350xAF0xF8"), 0.0 );
			}
		}
		
		if(!containsByteSequenceIndex.isEmpty()){
			jedis.zadd("indexDataContains_0x350xAF0xF8", timeStampIndex.map);
		}
		
		if(!timeStampIndex.isEmpty()){
			jedis.zadd("indexTimestamp", timeStampIndex.map);
		}
		
		if(!sourceAddrIndex.isEmpty()){
			jedis.zadd("indexSourceAddr", sourceAddrIndex.map);
		}
		
		if(!destinationAddrIndex.isEmpty()){
			jedis.zadd("indexDestinationAddr", destinationAddrIndex.map);
		}
		
		if(!sourcePortIndex.isEmpty()){
			jedis.zadd("indexSourcePort", sourcePortIndex.map);
		}
		
		if(!sourcePortIndex.isEmpty()){
			jedis.zadd("indexDestinationPort", destinationPortIndex.map);
		}
	}
}
