package com.imac;

import java.io.Serializable;
import java.sql.Timestamp;


public class CephDate implements Serializable {
	private Timestamp timestamp;
	private String ip;
	private Double ops;

	public Timestamp getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		String v = timestamp.substring(0, timestamp.indexOf("T"));
		String v2 = timestamp.substring(timestamp.indexOf("T") + 1, timestamp.indexOf("Z"));
		this.timestamp = Timestamp.valueOf(v + " " + v2 +":00");
		
//		this.timestamp = Timestamp.valueOf(timestamp.substring(0 , timestamp.lastIndexOf(".")));
	}

	public String getIP() {
		return ip;
	}

	public void setIP(String ip) {
		this.ip = ip;
	}

	public Double getOPS() {
		return ops;
	}

	public void setOPS(Double ops) {
		this.ops = ops;
	}
}