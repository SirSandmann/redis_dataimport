package de.hsh.inform.dbp_project_readdata;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class TestQueries {
	static JedisPool pool = new JedisPool(new JedisPoolConfig(), "localhost");

	public static void main(String[] args) {
		try (Jedis jedis = pool.getResource()) {
			// for measuring time
			long startTime = System.nanoTime();

			// activeConnections(jedis, "898691696", "898692070");
			// hostsConntectedTo(jedis, "209132068026");
			// hostIncomingConnectionsWellknown(jedis);
			// connectionOutsideHost(jedis);
			// getAllContaining(jedis);
			getDatavolumeBetweenConnection(jedis, "206132025071", "172016113204");

			long stopTime = System.nanoTime();
			System.out.println("\n-------------- Duration ---------------------");
			System.out.println(
					TimeUnit.MILLISECONDS.convert(stopTime - startTime, TimeUnit.NANOSECONDS) + " Milliseconds");
		}
	}

	// query 1
	public static void activeConnections(Jedis jedis, String t1, String t2) {
		Set<String> resultCounters = new HashSet<String>();
		Set<String> keysTimeStamps = jedis.zrangeByScore("index:timestamp", t1, t2);
		// get the numbers for all the connections
		for (String s : keysTimeStamps) {
			resultCounters.addAll(jedis.lrange(s, 0, -1));
		}

		// get the source and address values
		for (String s : resultCounters) {
			System.out.println(
					jedis.hget("meta:" + s, App.sourceAddr) + "\t and " + jedis.hget("meta:" + s, App.destinationAddr));
		}
		System.out.println("Total:" + resultCounters.size());
	}

	// query 2
	public static void hostsConntectedTo(Jedis jedis, String ip) {
		Integer port = 80;
		List<Set<String>> groupKeys = new ArrayList<Set<String>>();
		Set<String> resultCountersDestination = new HashSet<String>();
		Set<String> resultCountersSource = new HashSet<String>();
		Set<String> resultsCountersAll = new HashSet<String>();
		Set<String> results = new HashSet<String>();

		groupKeys.add(jedis.zrangeByScore("index:" + App.sourceAddr, ip, ip));
		groupKeys.add(jedis.zrangeByScore("index:" + App.sourcePort, port, port));
		groupKeys.add(jedis.zrangeByScore("index:" + App.destinationAddr, ip, ip));
		groupKeys.add(jedis.zrangeByScore("index:" + App.destinationPort, port, port));

		// Source get all IPS that are in the port and addr of source
		resultCountersSource.addAll(jedis.lrange(groupKeys.get(0).toString().replace(",", "").replace("[", "") // remove
																												// the
																												// right
																												// bracket
				.replace("]", ""), 0, -1));

		resultCountersSource.retainAll(jedis.lrange(groupKeys.get(1).toString().replace(",", "").replace("[", "") // remove
																													// the
																													// right
																													// bracket
				.replace("]", ""), 0, -1));

		// Destiation get all IPS that are in the port and addr of source
		resultCountersDestination.addAll(jedis.lrange(groupKeys.get(2).toString().replace(",", "") // remove
				.replace("[", "") // remove the right bracket
				.replace("]", ""), 0, -1));

		resultCountersDestination.retainAll(jedis.lrange(groupKeys.get(3).toString().replace(",", "") // remove
				.replace("[", "") // remove the right bracket
				.replace("]", ""), 0, -1));

		// Add Source and Destination to a single set -->remove automatically
		// duplicates
		resultsCountersAll.addAll(resultCountersDestination);
		resultsCountersAll.addAll(resultCountersSource);

		// get the source and address values
		for (String s : resultsCountersAll) {
			String src = jedis.hget("meta:" + s, App.sourceAddr);
			String dest = jedis.hget("meta:" + s, App.destinationAddr);

			// remove all non numeric values
			if (!IndexData.convertIpAdress(src.replaceAll("[^\\w.]+", "")).equals(ip)) {
				results.add(src);
			} else {
				results.add(dest);
			}
		}

		for (String s : results) {
			System.out.println(s);
		}

		System.out.println("Total:" + results.size());

	}

	public static void hostIncomingConnectionsWellknown(Jedis jedis) {
		List<Set<String>> groupKeys = new ArrayList<Set<String>>();
		Set<String> resultCountersAddr = new HashSet<String>();
		Set<String> resultCountersPort = new HashSet<String>();
		Set<String> result = new HashSet<String>();

		// all ports
		groupKeys.add(jedis.zrange("index:" + App.destinationAddr, 0, -1));
		groupKeys.add(jedis.zrangeByScore("index:" + App.destinationPort, 0, 1023));
		System.out.println(groupKeys);

		for (String s : groupKeys.get(0)) {
			resultCountersAddr.addAll(jedis.lrange(s, 0, -1));
		}

		for (String s : groupKeys.get(1)) {
			resultCountersPort.addAll(jedis.lrange(s, 0, -1));
		}

		resultCountersAddr.retainAll(resultCountersPort);

		// get address values
		for (String s : resultCountersAddr) {
			result.add(jedis.hget("meta:" + s, App.destinationAddr));
		}

		// remove duplicates --> set
		for (String s : result) {
			System.out.println(s);
		}

		System.out.println("Total:" + result.size());

	}

	public static void connectionOutsideHost(Jedis jedis) {
		List<Set<String>> groupKeys = new ArrayList<Set<String>>();
		Set<String> resultCounters = new HashSet<String>();
		Set<String> result = new HashSet<String>();

		groupKeys.add(jedis.zrangeByScore("index:" + App.destinationAddr, "0", "10000000000"));
		groupKeys.add(jedis.zrangeByScore("index:" + App.destinationAddr, "10255255255", "172160000000"));
		groupKeys.add(jedis.zrangeByScore("index:" + App.destinationAddr, "172310255255", "192168000000"));
		groupKeys.add(jedis.zrangeByScore("index:" + App.destinationAddr, "192168255255", "255255255255"));

		groupKeys.add(jedis.zrangeByScore("index:" + App.sourceAddr, "0", "10000000000"));
		groupKeys.add(jedis.zrangeByScore("index:" + App.sourceAddr, "10255255255", "172160000000"));
		groupKeys.add(jedis.zrangeByScore("index:" + App.sourceAddr, "172310255255", "192168000000"));
		groupKeys.add(jedis.zrangeByScore("index:" + App.sourceAddr, "192168255255", "255255255255"));

		for (int i = 0; i < groupKeys.size(); i++) {
			for (String s : groupKeys.get(i)) {
				resultCounters.addAll(jedis.lrange(s, 0, -1));
			}
		}

		for (String s : resultCounters) {
			if (!Boolean.valueOf(jedis.hget("meta:" + s, App.destinationAddrPriv))) {
				result.add(jedis.hget("meta:" + s, App.destinationAddr));
			}

			if (!Boolean.valueOf(jedis.hget("meta:" + s, App.sourceAddrPriv))) {
				result.add(jedis.hget("meta:" + s, App.sourceAddr));
			}
		}

		// remove duplicates --> set
		for (String s : result) {
			System.out.println(s);
		}

		System.out.println("Total:" + result.size());
	}

	public static void getAllContaining(Jedis jedis) {
		Set<String> indexContains = new HashSet<String>();
		Set<String> resultCounters = new HashSet<String>();
		indexContains.addAll(jedis.zrangeByScore("index:" + "dataContains_0x350xAF0xF8", 1, 1));

		// remove duplicates --> set
		for (String s : indexContains) {
			resultCounters.addAll(jedis.lrange(s, 0, -1));
		}

		for (String s : resultCounters) {
			System.out.println("Package with Counter: " + s);
		}

	}

	public static void getDatavolumeBetweenConnection(Jedis jedis, String ip1, String ip2) {
		double dataVolume = 0;
		double duration = 0;
		List<Set<String>> groupKeys = new ArrayList<Set<String>>();
		Set<String> resultCounters = new HashSet<String>();
		Set<Double> resultTimeStamp = new HashSet<Double>();

		// Source Addr
		groupKeys.add(jedis.zrangeByScore("index:" + App.sourceAddr, ip1, ip1));
		groupKeys.add(jedis.zrangeByScore("index:" + App.sourceAddr, ip2, ip2));
		groupKeys.add(jedis.zrangeByScore("index:" + App.destinationAddr, ip1, ip1));
		groupKeys.add(jedis.zrangeByScore("index:" + App.destinationAddr, ip2, ip2));

		for (int i = 0; i < groupKeys.size(); i++) {
			for (String s : groupKeys.get(i)) {
				resultCounters.addAll(jedis.lrange(s, 0, -1));
			}
		}

		for (String s : resultCounters) {
			String destAddr = IndexData
					.convertIpAdress(jedis.hget("meta:" + s, App.destinationAddr).replaceAll("[^\\w.]+", ""));
			String sourceAddr = IndexData
					.convertIpAdress(jedis.hget("meta:" + s, App.sourceAddr).replaceAll("[^\\w.]+", ""));

			if ((destAddr.equals(ip1) || destAddr.equals(ip2)) && (sourceAddr.equals(ip1) || sourceAddr.equals(ip2))) {
				dataVolume += Double.valueOf(jedis.hget("data:" + s, "dataLength"));
				resultTimeStamp.add(Double.valueOf(jedis.hget("meta:" + s, "timestamp")));
			}
		}

		double max = resultTimeStamp.stream().mapToDouble((x) -> x).max().orElseThrow(IllegalStateException::new);
		double min = resultTimeStamp.stream().mapToDouble((x) -> x).min().orElseThrow(IllegalStateException::new);

		duration = max - min;

		System.out.println("Duration: " + duration);
		System.out.println("Datavolume: " + dataVolume + "Bytes");
		System.out.printf("%.2f :Bytes pro Minute", duration * 100 * 60 / dataVolume);
	}

}
