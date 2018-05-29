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
		elements.add(new Bin("balance", Value.get(txn.getBalance())));
		elements.add(new Bin("txnId", Value.get(txn.getTxnId())));
		elements.add(new Bin("arrivalTime", Value.get(txn.getArrivalDate())));
		elements.add(new Bin("txnId", Value.get(txn.getTxnId().toString())));
		elements.add(new Bin("txnAmt", Value.get(txn.getTxnAmt())));
		elements.add(new Bin("prevRealId", Value.get(txn.getPreviousTxnId())));
		

		
		return elements.toArray(new Bin[0]);
	}
	
	public Map<String, Object> toMap(DataNode txn) {
		Map<String, Object> elements = new HashMap<String, Object>();
		elements.put("balance", txn.getBalance());
		elements.put("txnId", txn.getTxnId());
		elements.put("arrivalTime", txn.getArrivalDate());
		//elements.put("txnId", txn.getTxnId().toString());
		elements.put("txnAmt", txn.getTxnAmt());
		
		elements.put("prevRealId", txn.getPreviousTxnId());

		
		return elements;
	}
	
	public DataNode fromMap(Map<String, Object> elements) {
		DataNode node = new DataNode();
		node.setBalance((Double)elements.get("balance"));
		node.setTxnId((String)elements.get("txnId"));
		node.setArrivalDate((Long)elements.get("arrivalTime"));
		node.setTxnAmt((Double)elements.get("txnAmt"));
		node.setPreviousTxnId((String)elements.get("prevRealId"));
	

		
		return node;
	}
}
