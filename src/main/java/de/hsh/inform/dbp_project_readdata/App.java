package de.hsh.inform.dbp_project_readdata;

import java.io.EOFException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeoutException;

import org.pcap4j.core.NotOpenException;
import org.pcap4j.core.PcapHandle;
import org.pcap4j.core.PcapNativeException;
import org.pcap4j.core.Pcaps;
import org.pcap4j.packet.ArpPacket;
import org.pcap4j.packet.EthernetPacket;
import org.pcap4j.packet.FragmentedPacket;
import org.pcap4j.packet.IcmpV4CommonPacket;
import org.pcap4j.packet.IpV4Packet;
import org.pcap4j.packet.Packet;
import org.pcap4j.packet.TcpPacket;
import org.pcap4j.packet.UdpPacket;
import org.pcap4j.packet.namednumber.EtherType;
import org.pcap4j.packet.namednumber.IpNumber;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * A small sample application that reads a pcap file and interprets its
 * contents. Be aware that more cases might occur! Your database model should be
 * able to cope with other cases as well.
 *
 * The pcap file on the skripte server is compressed and must be uncompressed
 * before reading it with this application.
 */
public class App {
	static JedisPool pool = new JedisPool(new JedisPoolConfig(), "localhost");

	static final String destinationAddr = "destinationAddr";
	static final String sourceAddr = "sourceAddr";

	static final String rawData = "rawData";

	static final String sourcePort = "sourcePort";
	static final String destinationPort = "destinationPort";

	static final String sourcePortWellKnown = "sourcePortWellKnown";
	static final String destinationPortWellKnown = "destinationPortWellKnown";

	static final String sourceAddrPriv = "sourceAddrPriv";
	static final String destinationAddrPriv = "destinationAddrPriv";

	public static void main(String[] args)
			throws PcapNativeException, EOFException, TimeoutException, NotOpenException {

		String filename = System.getProperty("user.dir") + "/data_dumps/tcpdump";
		PcapHandle handle = Pcaps.openOffline(filename);
		int cnt = 0;
		for (;;) {
			// loop over all packets in the pcap file

			Packet packet = null;
			try {
				packet = handle.getNextPacketEx();
			} catch (PcapNativeException ex) {
				System.out.println(ex.toString());
				// file is truncated. ignore exception
			}
			if (packet == null)
				break;

			// read basic packet data: timestamp and length
			long ts = handle.getTimestampInts();
			int ms = handle.getTimestampMicros();
			int len = packet.length();

			// create Packagedata Class and put time in meta and data in data
			PackageData data = new PackageData();
			data.metaData.put("timestamp", ts + ms + "");
			data.dataData.put("dataLength", len + "");

			System.out.printf("New Packet. TS=%d/%d, len=%d\n", ts, ms, len);

			// this caputure only contains ethernet packets
			// for other data, this must be modified.
			EthernetPacket ether = packet.get(EthernetPacket.class);
			handleEthernetPacket(ether, data, cnt, "  ");
			cnt++;
		}
		handle.close();
		System.out.println("Number of packets: " + cnt);
		pool.destroy();
	}

	public static void handleEthernetPacket(EthernetPacket ether, PackageData data, int cnt, String prefix) {
		// depending on the ethernet type, interpret the contents of the
		// ethernet
		// frame in different ways
		EtherType etherType = ether.getHeader().getType();
		System.out.println(prefix + "EtherType " + etherType);

		if (etherType.equals(EtherType.IPV4)) {
			IpV4Packet ipv4 = ether.getPayload().get(IpV4Packet.class);

			// if valid ip4 package
			if (ipv4.getHeader().hasValidChecksum(true)) {
				// Destination and Source Address just accessable in the ip
				// object
				data = addDestinationAndSourceAddr(ipv4, data);
				// import all recieved data
				importDataIntoRedis(cnt, handleIpV4Packet(ipv4, data, prefix + "  "));
			}

		} else if (ether.getHeader().getType().equals(EtherType.ARP)) {
			// NO IP4 Package --> destination and sourceAddress just available
			// in the header
			ArpPacket arp = ether.getPayload().get(ArpPacket.class);
			// finally import all data
			importDataIntoRedis(cnt, handleArpPacket(arp, data, prefix + "  "));

		} else {
			System.out.println(prefix + "EtherType unknown, no further processing");
			// unknown type, drop all data
		}

	}

	public static PackageData handleIpV4Packet(IpV4Packet ipv4, PackageData data, String prefix) {
		/*
		 * TODO: Replace all name of KEys with constants
		 */
		// Adding Destination and Source
		IpNumber ipnum = ipv4.getHeader().getProtocol();

		System.out.printf(prefix + "Ip Protocol Number: %s\n", ipnum.toString());
		if (ipv4.getPayload() instanceof FragmentedPacket) {
			System.out.println(prefix + "Fragemented Packet");
		} else if (ipnum.equals(IpNumber.TCP)) {
			TcpPacket tcp = ipv4.getPayload().get(TcpPacket.class);
			handleTcpPacket(tcp, data, prefix + "  ");
		} else if (ipnum.equals(IpNumber.UDP)) {
			UdpPacket udp = ipv4.getPayload().get(UdpPacket.class);
			handleUdpPacket(udp, data, prefix + "  ");
		} else if (ipnum.equals(IpNumber.ICMPV4)) {
			IcmpV4CommonPacket icmp = ipv4.getPayload().get(IcmpV4CommonPacket.class);
			handleIcmpPacket(icmp, data, prefix + "  ");
		} else {
			System.out.println(prefix + "Unknown protocol, no further processing");
		}

		return data;
	}

	public static PackageData addDestinationAndSourceAddr(IpV4Packet ipv4, PackageData data) {
		// meta data
		data.metaData.put(destinationAddr, ipv4.getHeader().getDstAddr() + "");
		data.metaData.put(sourceAddr, ipv4.getHeader().getSrcAddr() + "");
		return data;
	}

	public static PackageData handleArpPacket(ArpPacket arp, PackageData data, String prefix) {
		System.out.println(prefix + "Storing ARP packet");
		// meta
		data.metaData.put(sourceAddr, arp.getHeader().getSrcProtocolAddr() + "");
		data.metaData.put(destinationAddr, arp.getHeader().getDstProtocolAddr() + "");
		// data
		data.dataData.put(rawData, arp.getRawData() + "");
		return data;
	}

	public static PackageData handleTcpPacket(TcpPacket tcp, PackageData data, String prefix) {
		System.out.println(prefix + "Storing TCP packet");
		// meta
		data.metaData.put(sourcePort, tcp.getHeader().getSrcPort() + "");
		data.metaData.put(destinationPort, tcp.getHeader().getDstPort() + "");
		// data
		data.dataData.put(rawData, tcp.getRawData() + "");
		return data;
	}

	public static PackageData handleUdpPacket(UdpPacket udp, PackageData data, String prefix) {
		System.out.println(prefix + "Storing UDP packet");
		// meta
		data.metaData.put(sourcePort, udp.getHeader().getSrcPort() + "");
		data.metaData.put(destinationPort, udp.getHeader().getDstPort() + "");
		// data
		data.dataData.put(rawData, udp.getRawData() + "");
		return data;
	}

	public static PackageData handleIcmpPacket(IcmpV4CommonPacket icmp, PackageData data, String prefix) {
		System.out.println(prefix + "Storing ICMP packet");
		// data
		data.dataData.put(rawData, icmp.getRawData() + "");
		return data;
	}

	public static PackageData preprocessing(PackageData data) {
		data = addWellknown(data);
		data = addPrivate(data);
		return data;
	}

	public static boolean checkWellknown(String s) {
		// if port <= 1023 it is wellknown
		// replace all non digits in the ports
		return Integer.parseInt(s.replaceAll("\\D+", "")) <= 1023;
	}

	public static PackageData addWellknown(PackageData data) {
		// if source Port exists check if < 1023 and add value to map
		if (data.metaData.containsKey(sourcePort) && checkWellknown(data.metaData.get(sourcePort))) {
			data.metaData.put(sourcePortWellKnown, "true");
		} else if (data.metaData.containsKey(sourcePort)) {
			data.metaData.put(sourcePortWellKnown, "false");
		}

		if (data.metaData.containsKey(destinationPort) && checkWellknown(data.metaData.get(destinationPort))) {
			data.metaData.put(destinationPortWellKnown, "true");
		} else if (data.metaData.containsKey(destinationPort)) {
			data.metaData.put(destinationPortWellKnown, "false");
		}

		return data;
	}

	public static boolean checkPrivateIp(String s) {
		// check if in specific range
		try {
			return InetAddress.getByName(s.replace("/", "")).isSiteLocalAddress();
		} catch (UnknownHostException e) {
			e.printStackTrace();
			//hard break in case of wrong ip
			System.exit(0);
			return false;
		}
	}

	public static PackageData addPrivate(PackageData data) {
		if (data.metaData.containsKey(sourceAddr)) {
			if (checkPrivateIp(data.metaData.get(sourceAddr))) {
				data.metaData.put(sourceAddrPriv, "true");
			} else {
				data.metaData.put(sourceAddrPriv, "false");
			}
		}

		if (data.metaData.containsKey(destinationAddr)) {
			if (checkPrivateIp(data.metaData.get(destinationAddr))) {
				data.metaData.put(destinationAddrPriv, "true");
			} else {
				data.metaData.put(destinationAddrPriv, "false");
			}
		}
		return data;
	}

	public static void importDataIntoRedis(int cnt, PackageData data) {
		data = preprocessing(data);
		try (Jedis jedis = pool.getResource()) {
			jedis.hmset(cnt + ":meta", data.metaData);
			jedis.hmset(cnt + ":data", data.dataData);
		}
	}

}
