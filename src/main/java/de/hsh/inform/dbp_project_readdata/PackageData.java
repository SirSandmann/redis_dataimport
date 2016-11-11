package de.hsh.inform.dbp_project_readdata;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

public class PackageData {

	Map<String, String> metaData = new HashMap<String, String>();
	Map<String, String> dataData = new HashMap<String, String>();
	
	public static boolean checkPrivateIp(String s) {
		// check if in specific range
		try {
			return InetAddress.getByName(s.replace("/", "")).isSiteLocalAddress();
		} catch (UnknownHostException e) {
			e.printStackTrace();
			// hard break in case of wrong ip
			System.exit(0);
			return false;
		}
	}

	public static PackageData addPrivate(PackageData data) {
		if (data.metaData.containsKey(App.sourceAddr)) {
			if (checkPrivateIp(data.metaData.get(App.sourceAddr))) {
				data.metaData.put(App.sourceAddrPriv, "true");
			} else {
				data.metaData.put(App.sourceAddrPriv, "false");
			}
		}

		if (data.metaData.containsKey(App.destinationAddr)) {
			if (checkPrivateIp(data.metaData.get(App.destinationAddr))) {
				data.metaData.put(App.destinationAddrPriv, "true");
			} else {
				data.metaData.put(App.destinationAddrPriv, "false");
			}
		}
		return data;
	}
	
	public static boolean checkWellknown(String s) {
		// if port <= 1023 it is wellknown
		// replace all non digits in the ports
		return Integer.parseInt(s.replaceAll("\\D+", "")) <= 1023;
	}

	public static PackageData addWellknown(PackageData data) {
		// if source Port exists check if < 1023 and add value to map
		if (data.metaData.containsKey(App.sourcePort) && checkWellknown(data.metaData.get(App.sourcePort))) {
			data.metaData.put(App.sourcePortWellKnown, "true");
		} else if (data.metaData.containsKey(App.sourcePort)) {
			data.metaData.put(App.sourcePortWellKnown, "false");
		}

		if (data.metaData.containsKey(App.destinationPort) && checkWellknown(data.metaData.get(App.destinationPort))) {
			data.metaData.put(App.destinationPortWellKnown, "true");
		} else if (data.metaData.containsKey(App.destinationPort)) {
			data.metaData.put(App.destinationPortWellKnown, "false");
		}

		return data;
	}
}
