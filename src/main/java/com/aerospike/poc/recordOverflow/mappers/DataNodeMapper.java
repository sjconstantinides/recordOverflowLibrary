package com.aerospike.poc.recordOverflow.mappers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aerospike.client.Bin;
import com.aerospike.client.Value;
import com.aerospike.poc.recordOverflow.model.DataNode;

public class DataNodeMapper {
	public Bin[] toRecord(DataNode txn) {
		List<Bin> elements = new ArrayList<Bin>();
		elements.add(new Bin("sum", Value.get(txn.getSum())));
		elements.add(new Bin("eltId", Value.get(txn.getEltId())));
		elements.add(new Bin("arrivalTime", Value.get(txn.getArrivalDate())));
		elements.add(new Bin("eltId", Value.get(txn.getEltId().toString())));
		elements.add(new Bin("eltVal", Value.get(txn.getEltValue())));
		elements.add(new Bin("prevEltId", Value.get(txn.getPreviousEltId())));
		

		
		return elements.toArray(new Bin[0]);
	}
	
	public Map<String, Object> toMap(DataNode txn) {
		Map<String, Object> elements = new HashMap<String, Object>();
		elements.put("sum", txn.getSum());
		elements.put("eltId", txn.getEltId());
		elements.put("arrivalTime", txn.getArrivalDate());
		//elements.put("txnId", txn.getTxnId().toString());
		elements.put("eltVal", txn.getEltValue());
		
		elements.put("prevEltId", txn.getPreviousEltId());

		
		return elements;
	}
	
	public DataNode fromMap(Map<String, Object> elements) {
		DataNode node = new DataNode();
		node.setSum((Double)elements.get("sum"));
		node.setEltId((String)elements.get("eltId"));
		node.setArrivalDate((Long)elements.get("arrivalTime"));
		node.setEltValue((Double)elements.get("eltVal"));
		node.setPreviousEltId((String)elements.get("prevEltId"));
	

		
		return node;
	}
}
