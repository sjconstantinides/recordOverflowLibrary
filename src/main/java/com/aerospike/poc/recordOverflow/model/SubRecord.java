package com.aerospike.poc.recordOverflow.model;

import java.util.Map;
import java.util.TreeMap;

public class SubRecord {
	private Map<String, DataNode> transactionGraph;
	private String head;
	
	public SubRecord( Map<String, DataNode> inGraph, String inHead ){
		
		transactionGraph = inGraph;
		head = inHead;
		//Operation mapOp = transactionGraph.getByIndex("transactionGraph", -1, MapReturnType.VALUE);
		
	}
	

	public SubRecord() {
		// TODO Auto-generated constructor stub
	}


	public Map<String, DataNode> getTransactionGraph() {
		return transactionGraph;
	}
	public void setTransactionGraph(TreeMap<String, DataNode> transactionGraph) {
		this.transactionGraph = transactionGraph;
	}
	public String getHead() {
		return head;
	}
	public void setHead(String head) {
		this.head = head;
	}

}
