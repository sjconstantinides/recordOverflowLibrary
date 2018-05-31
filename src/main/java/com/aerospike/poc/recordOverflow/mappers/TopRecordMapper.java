package com.aerospike.poc.recordOverflow.mappers;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
//import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aerospike.client.Bin;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapWriteMode;
import com.aerospike.poc.recordOverflow.model.DataNode;
import com.aerospike.poc.recordOverflow.model.TopRecord;

public class TopRecordMapper {
//	public String getBinNameForField(String field) {
//		switch (field) {
//		case "number": return "number";
//		case "nameOnAccount"
//		}
//	}
	
	private final String binName = "dataGraph";
	private MapOperation mapOpp = new MapOperation();
	private MapPolicy mapPolicy;
	private Bin dGraph;
	
	
	public TopRecord fromRecord(Record record) {
		Map <String,DataNode> inGraph;
		
		TopRecord result = null;
		if (record != null) {
			result = new TopRecord();
			result.setNumOverflow(record.getInt("overflow"));
			result.setNameOnAccount(record.getString("name"));
			result.setUserKey(record.getString("userKey"));
			result.setDateOpened(new Date(record.getLong("opened")));
	
			result.setEltIdMapper((ArrayList<ArrayList<String>>)record.getList("eltIdMapper"));
			result.setArrMapper((ArrayList<ArrayList<String>>)record.getList("arrMapper"));
			inGraph = buildGraphFromDB(record);
			result.setDataGraph(inGraph);
			result.setHead(record.getString("head"));
			result.setCompensatingNodeIdx(record.getInt("compNodeIdx"));
			
		}
		return result;
	}
	
	public Bin[] toRecord(TopRecord account) {
		DataNodeMapper tMapper = new DataNodeMapper();
		Map <String, Map<String, Object>> graph = new HashMap<String, Map<String, Object>>();
		for (DataNode curNode: account.getDataGraph().values()) {
			graph.put(curNode.getEltId(), tMapper.toMap(curNode));
		}
		dGraph = new Bin(binName, graph);
		mapPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE);
		mapOpp.setMapPolicy(mapPolicy, binName);
		
		List<Bin> elements = new ArrayList<Bin>();
		elements.add(new Bin("overflow", Value.get(account.getNumOverflow())));
		elements.add(new Bin("name", Value.get(account.getNameOnAccount())));
		elements.add(new Bin("userKey", Value.get(account.getUserKey())));
		elements.add(new Bin("opened", Value.get(account.getDateOpened().getTime())));
		elements.add(new Bin("eltIdMapper", Value.get(account.getEltIdMapper())));
		elements.add(new Bin("arrMapper", Value.get(account.getArrMapper())));
		elements.add(dGraph);
		elements.add(new Bin("head", Value.get(account.getHead())));
		elements.add(new Bin("compNodeIdx", Value.get(account.getCompensatingNodeIdx())));
		
	
		
		
		return elements.toArray(new Bin[0]);
	}
	
	
	/*
	 * utility function used to load the individual elements stored in a non Java serialied map within the bin txnGraph, to 
	 * build and return the TopRecord Data Graph
	 */
	public HashMap<String, DataNode> buildGraphFromDB(Record record) {
		HashMap<String, DataNode> retGraph = new HashMap<String, DataNode>();
		Map <String, Map<String, Object>> graph = ( Map<String, Map<String, Object>>)record.getMap("txnGraph");
		Iterator<Map<String, Object>> graphItr = graph.values().iterator();
		DataNode curNode;
		DataNodeMapper tMapper = new DataNodeMapper();
		
		
		
		 while ( graphItr.hasNext()){
			curNode = tMapper.fromMap(graphItr.next());
			retGraph.put(curNode.getEltId(), curNode);
		}

		return retGraph;
	}
}
