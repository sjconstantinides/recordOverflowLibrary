package com.aerospike.poc.recordOverflow.model;

import java.util.Map;
import java.util.TreeMap;

public class SubRecord {
	private Map<String, DataNode> dataGraph;
	private String head;
	
	public SubRecord( Map<String, DataNode> inGraph, String inHead ){
		
		dataGraph = inGraph;
		head = inHead;
		
	}
	

	public SubRecord() {
		// TODO Auto-generated constructor stub
	}


	public Map<String, DataNode> getDataGraph() {
		return dataGraph;
	}
	public void setDataGraph(TreeMap<String, DataNode> dGraph) {
		this.dataGraph = dGraph;
	}
	public String getHead() {
		return head;
	}
	public void setHead(String head) {
		this.head = head;
	}

}
