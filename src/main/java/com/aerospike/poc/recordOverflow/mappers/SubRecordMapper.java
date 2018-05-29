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

	
	private final String binName = "txnGraph";
	private MapOperation mapOpp = new MapOperation();
	private MapPolicy mapPolicy;
	private Bin tGraph;
	
	
	public SubRecord fromRecord(Record record) {
		
		SubRecord result = null;
		if (record != null) {
			result = new SubRecord();
			
	
		
			//result.setTransactionGraph((Map<String, TransactionNode>)record.getMap("txnGraph", Collections.<String,TransactionNode>emptyMap().getClass());
			result.setHead(record.getString("head"));
			
			
			//(Class<? extends Map>) Collections.<String,TransactionNode>emptyMap().getClass())
			//transactionGraph = record.getMap("txnGraph", Collections.<String,TransactionNode>emptyMap().getClass()); 
			
		}
		return result;
	}
	
	public Bin[] toRecord(SubRecord account) {
		DataNodeMapper tMapper = new DataNodeMapper();
		Map <String, Map<String, Object>> graph = new HashMap<String, Map<String, Object>>();
		for (DataNode curNode: account.getTransactionGraph().values()) {
			graph.put(curNode.getTxnId(), tMapper.toMap(curNode));
		}
		//tGraph = new Bin(binName, account.getTransactionGraph());
		tGraph = new Bin(binName, graph);
		//tGraph = new Bin(binName, account.getTransactionGraph());
		mapPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE);
		mapOpp.setMapPolicy(mapPolicy, binName);
		
		List<Bin> elements = new ArrayList<Bin>();
		
		//elements.add(new Bin("txnGraph", Value.get(account.getTransactionGraph())));
		elements.add(tGraph);
		elements.add(new Bin("head", Value.get(account.getHead())));
		
		return elements.toArray(new Bin[0]);
	}
}
