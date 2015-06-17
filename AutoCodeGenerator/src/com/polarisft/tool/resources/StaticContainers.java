package com.polarisft.tool.resources;

import java.util.Map;

public class StaticContainers {
	private static  Map<String, String> myMap;

	public static Map<String, String> getMyMap() {
		return myMap;
	}

	public static void setMyMap(Map<String, String> myMap) {
		StaticContainers.myMap = myMap;
	}
}
