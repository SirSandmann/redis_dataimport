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

			//activeConnections(jedis, "898691696", "898692070");
			hostsConntectedTo(jedis, "209143215163");

			long stopTime = System.nanoTime();
			System.out.println("\n-------------- Duration ---------------------");
			System.out.println(                
					TimeUnit.MILLISECONDS.convert(stopTime - startTime, TimeUnit.NANOSECONDS) + " Milliseconds");
		}
	}

	public static void activeConnections(Jedis jedis, String t1, String t2) {
		Set<String> resultCounters = new HashSet<String>();
		Set<String> keysTimeStamps = jedis.zrangeByScore("index:timestamp", t1, t2);
		// get the numbers for all the connections
		for (String s : keysTimeStamps) {
			resultCounters.addAll(jedis.lrange(s, 0, -1));
		}

		// get the source and address values
		for (String s : resultCounters) {
			System.out.println(jedis.hget("meta:" + s, App.sourceAddr) + " and "
					+ jedis.hget("meta:" + s, App.destinationAddr) + "\n");
		}
	}

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
		resultCountersSource.addAll(jedis.lrange(groupKeys.get(0).toString().replace(",", "") // remove
																								// the
																								// commas
				.replace("[", "") // remove the right bracket
				.replace("]", ""), 0, -1));
		resultCountersSource.retainAll(jedis.lrange(groupKeys.get(1).toString().replace(",", "") // remove
																									// the
																									// commas
				.replace("[", "") // remove the right bracket
				.replace("]", ""), 0, -1));

		// Destiation get all IPS that are in the port and addr of source
		resultCountersDestination.addAll(jedis.lrange(groupKeys.get(2).toString().replace(",", "") // remove
																									// the
																									// commas
				.replace("[", "") // remove the right bracket
				.replace("]", ""), 0, -1));
		resultCountersDestination.retainAll(jedis.lrange(groupKeys.get(3).toString().replace(",", "") // remove
																										// the
																										// commas
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
			if (!src.replaceAll("\\D+", "").equals(ip)) {
				results.add(src);
			} else {
				results.add(dest);
			}
		}
		System.out.println(results);

	}

}
