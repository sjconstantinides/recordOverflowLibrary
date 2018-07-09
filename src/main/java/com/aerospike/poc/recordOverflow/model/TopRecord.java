package com.aerospike.poc.recordOverflow.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapWriteMode;
import com.aerospike.poc.recordOverflow.mappers.SubRecordMapper;
import com.aerospike.poc.recordOverflow.mappers.TopRecordMapper;





/**
 * TopRecord is the over-arching class to demonstrate how record overflow is handled in Aerospike
 * This is used in the logic to create SubRecords to handle the record overflow. 
 * @author Stefan
 *
 */
public class TopRecord {

	private int numOverflow;

	private String userKey;			// SJC 
	private ArrayList<ArrayList<String>> eltIdMapper;
	private ArrayList<ArrayList<String>> arrMapper;
	private Map<String,DataNode> dataGraph;
	private String head;		// key to the head DataNode of the graph
	
	
	private final String binName = "dataGraph";
	private MapOperation mapOpp = new MapOperation();
	private MapPolicy mapPolicy;
	private Bin dGraph;
	
	private final SubRecordMapper mapper = new SubRecordMapper();
	private IAerospikeClient database;
	private String keySpace;
	private String lockNameSpace;
	
	

	public TopRecord(){
		eltIdMapper = new ArrayList< ArrayList< String>>();
		arrMapper = new ArrayList< ArrayList< String >>();
		dataGraph = new HashMap<String, DataNode>();
		head = null;
		numOverflow = 0;
		dGraph = new Bin(binName, dataGraph);
		mapPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE);
		mapOpp.setMapPolicy(mapPolicy, binName);
		database = null;
		keySpace = null;
		lockNameSpace = null;
	}
	
	public String getLockNameSpace() {
		return lockNameSpace;
	}

	public void setLockNameSpace(String lockNameSpace) {
		this.lockNameSpace = lockNameSpace;
	}

	public TopRecord(IAerospikeClient database2, String keySpace2) {
		this();
		database = database2;
		keySpace = keySpace2;
		lockNameSpace = new String("lock_ns");
	}
	
	public TopRecord(IAerospikeClient database2, String keySpace2, String lockName) {
		this();
		database = database2;
		keySpace = keySpace2;
		lockNameSpace = lockName;
	}

	public int getNumOverflow() {
		return numOverflow;
	}
	public void setNumOverflow(int number) {
		this.numOverflow = number;
	}

	public String getUserKey() {
		return userKey;
	}
	public void setUserKey(String uKey) {
		this.userKey = uKey;
	}

	public ArrayList<ArrayList<String>> getEltIdMapper() {
		return eltIdMapper;
	}
	public void setEltIdMapper(ArrayList<ArrayList<String>> txnIdMapper) {
		this.eltIdMapper = txnIdMapper;
	}
	
	public ArrayList<ArrayList<String>> getArrMapper() {
		return arrMapper;
	}
	public void setArrMapper(ArrayList<ArrayList<String>> arrMapper) {
		this.arrMapper = arrMapper;
	}
	public Map<String,DataNode> getDataGraph() {
		return dataGraph;
	}
	public void setDataGraph(Map<String, DataNode> map) {
		this.dataGraph = map;
	}
	public String getHead() {
		return head;
	}
	public void setHead(String head) {
		this.head = head;
	}





	public void initializeMappers(DataNode newElt) {
		ArrayList<String> insertList = new ArrayList<String>();
		insertList.add(userKey);
		
		insertList.add(newElt.getEltId());
		eltIdMapper.add(0,insertList);
		
		insertList.remove(1);
		insertList.add(Long.toString(newElt.getArrivalDate()));
		arrMapper.add(0,insertList);
		
		
	}

	/*
	 * used primarily for creation, in real application might be able to do this with a mapOperation command
	 * or a better way to replace a value in a list
	 * 
	 * the mapper lists contain only two elements: the first key and the last key, 
	 * Use eltIdMapper if the element IDs are sequential, Use arrMapper otherwise
	 */
	public void updateMappers(DataNode newElt) {
		
		if ( eltIdMapper.isEmpty()) {
			initializeMappers(newElt);
			return;
		}
		ArrayList<String> insertList = eltIdMapper.get(0);
		
		if (insertList.size() >= 3){
			insertList.remove(2);
		}
		insertList.add(2, newElt.getEltId());
		eltIdMapper.remove(0);
		eltIdMapper.add(0, insertList);
		
		insertList = arrMapper.get(0);
		if (insertList.size() >= 3){
			insertList.remove(2);
		}
		insertList.add(2, Long.toString(newElt.getArrivalDate()));
		arrMapper.remove(0);
		arrMapper.add(0, insertList);
		
		
	}






	public void updateOverFlowMappers(String uKey) {
		
		eltIdMapper.get(0).remove(0);
		eltIdMapper.get(0).add(0,uKey);
		
		arrMapper.get(0).remove(0);
		arrMapper.get(0).add(0,uKey);
			
	}

	public void setDatabase(IAerospikeClient database2) {
		this.database = database2;
		
	}

	public void setNamespace(String nameSpace) {
		this.keySpace = nameSpace;
		
	}

	/*
	 * returns the found data node or null if not found
	 */
	public DataNode getElement(String eltId, String eltDate) {
		DataNode txn = dataGraph.get(eltId);
		if (txn != null){		// no overflow so in topRecord
			return ( txn);
		}
		TopRecordMapper topRecMapper = new TopRecordMapper();
		int subRecNum = findSubRecord( arrMapper, eltId, eltDate);	// use the arrival time Mapper
		if (subRecNum > 0){
			String subRecId = new String(userKey + "-" + subRecNum);
			Key key = new Key(keySpace, "subrecords", subRecId);
			Record record = database.get(null, key);
			Map<String,DataNode> localDataGraph = topRecMapper.buildGraphFromDB(record);
			return( localDataGraph.get(eltId));
		}
		
		return null;	
		
	}

	public int findSubRecord(ArrayList<ArrayList<String>> mapper, String eltId, String eltDate) {
		int subRecIdx = 0;
		boolean found = false;
		Iterator<ArrayList<String>> curList = mapper.iterator();
		ArrayList<String> curSubList;
		int eltDateNum = Integer.parseInt(eltDate);
		
		while (curList.hasNext() && !found) {
			curSubList = curList.next();
			if ( eltDateNum >= Integer.parseInt(curSubList.get(0)) && eltDateNum <= Integer.parseInt(curSubList.get(1))){
				found = true;
			}
			else {
				subRecIdx++;
			}
		}
		
		if (found) {
			return subRecIdx;
		}
		else {
			return 0;
		}
		
	}

	public void incrementNumOverflow() {
		numOverflow++;
		
	}
}
