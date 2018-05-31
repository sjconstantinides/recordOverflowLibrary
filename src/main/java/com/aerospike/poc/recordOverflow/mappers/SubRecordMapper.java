package com.aerospike.poc.recordOverflow.mappers;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
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
import com.aerospike.poc.recordOverflow.model.SubRecord;

public class SubRecordMapper {

	
	private final String binName = "dataGraph";
	private MapOperation mapOpp = new MapOperation();
	private MapPolicy mapPolicy;
	private Bin dGraph;
	
	
	public SubRecord fromRecord(Record record) {
		
		SubRecord result = null;
		if (record != null) {
			result = new SubRecord();
			
			result.setHead(record.getString("head"));
			
		}
		return result;
	}
	
	public Bin[] toRecord(SubRecord account) {
		DataNodeMapper tMapper = new DataNodeMapper();
		Map <String, Map<String, Object>> graph = new HashMap<String, Map<String, Object>>();
		for (DataNode curNode: account.getDataGraph().values()) {
			graph.put(curNode.getEltId(), tMapper.toMap(curNode));
		}
		dGraph = new Bin(binName, graph);
		mapPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE);
		mapOpp.setMapPolicy(mapPolicy, binName);
		
		List<Bin> elements = new ArrayList<Bin>();
		
		elements.add(dGraph);
		elements.add(new Bin("head", Value.get(account.getHead())));
		
		return elements.toArray(new Bin[0]);
	}
}
