package com.aerospike.poc.recordOverflow;

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
import com.aerospike.poc.recordOverflow.mappers.DataNodeMapper;
import com.aerospike.poc.recordOverflow.mappers.SubRecordMapper;
import com.aerospike.poc.recordOverflow.mappers.TopRecordMapper;
import com.aerospike.poc.recordOverflow.model.DataNode;
import com.aerospike.poc.recordOverflow.model.SubRecord;
import com.aerospike.poc.recordOverflow.model.TopRecord;

public class DataAccessMethods {
	
	private IAerospikeClient database;
	private String nameSpace;
	private String lockNameSpace;
	private String userKey;
	private final SubRecordMapper mapper = new SubRecordMapper();
	
	// TODO: make the following parameters that can be set via the .properties file
	private final static int SIZEOF_ELEMENT_NODE = 1500;
	private final long RECORD_SIZE = 131072;		// 128 KB
	private final int MAPPERS_SIZE = 72;
	private final int T_RECORD_SIZE = 48;	// 8 bytes per field
	private int BLOCK_MULTIPLIER = 1;
	
	public String getUserKey() {
		return userKey;
	}

	public void setUserKey(String uKey) {
		this.userKey = uKey;
	}

	public DataAccessMethods (IAerospikeClient db, String keySpace, String lockSpace){
		this.database = db;
		this.nameSpace = keySpace;
		this.lockNameSpace = lockSpace;
		this.userKey = null;
	}
	
	@SuppressWarnings("unchecked")
	// return null if the record is not found
	public DataNode retrieveHead( String acctId) {
		this.setUserKey(acctId);
		Key key = new Key(nameSpace, "toprecords", acctId);
		Record record = database.get(null, key);
		if (record == null) {
			return( null );
		}
		DataNodeMapper tMapper = new DataNodeMapper();
		Map<String, Map<String, Object>> graph = (Map<String, Map<String, Object>>)record.getMap("txnGraph");
		Map<String, Object> innerMap = graph.get(record.getString("head"));
		DataNode node = tMapper.fromMap(innerMap);
		
		
		return node;
	}
	
	/**
	 * retrieves the n most recent elements or null if the record is not found
	 * 
	 * the number of elements returned could exceed the top record and go into the overflow records
	 * there is logic to traverse the overflow nodes in order to return all the desired nodes
	 */
	public ArrayList<DataNode> retrievRecentElements( String uKey, int numElts ){
	
		HashMap<String, DataNode> dataGraph =null;
		this.setUserKey(uKey);
		Key key = new Key(nameSpace, "toprecords", uKey);
		Record record = database.get(null, key);
		if (record == null){
			return (null);
		}
		DataNode curNode, tmpNode;
		int subRecordCount = 1;
		int overflow = (int)record.getInt("overflow");
		TopRecordMapper topRecMapper = new TopRecordMapper();
		
		
		dataGraph = topRecMapper.buildGraphFromDB(record);
		
		
		 
		ArrayList<DataNode> retList = new ArrayList<DataNode>();
		int count;
		curNode = dataGraph.get(record.getString("head"));
		//curNode = dataGraph.get("Wed Dec 27 17:52:15 PST 2017");
		//System.out.println(dataGraph.size() + "total elements");


		for (count = 0; count < numElts && curNode != null; count++) {
			retList.add(curNode);
			tmpNode = dataGraph.get(curNode.getPreviousEltId());
			if (tmpNode == null && subRecordCount <= overflow) {
				String subRecId = new String(uKey + "-" + overflow--);
				key = new Key(nameSpace, "subrecords", subRecId);
				record = database.get(null, key);
				dataGraph = topRecMapper.buildGraphFromDB(record);;
				tmpNode = dataGraph.get(curNode.getPreviousEltId());
				//System.out.println("read in subRecord");
			}
			curNode = tmpNode;
		}
	
	
		
		return retList;
	}

	public void insertTest(String acctId, DataNode txn) {
		this.setUserKey(acctId);
		TopRecordMapper topRecMapper = new TopRecordMapper();
		TopRecord topRec = null;
		Key key = new Key(nameSpace, "toprecords", acctId);
		Key scKey = new Key(lockNameSpace, "lockset", acctId);
		Record record = database.get(null, key);
		if (record == null) {	// record does not exist, so create
			topRec = new TopRecord( database, nameSpace);
			topRec.setUserKey(acctId);
			this.userKey = acctId;
			topRec.setDateOpened(new Date());
			topRec.setNameOnAccount(new String("Test Name"));
		}
		else {
			
			topRec = topRecMapper.fromRecord(record);
			topRec.setDatabase(database);
			topRec.setNamespace(nameSpace);
		}
		
		ArrayList <DataNode> insertNodes;
		
		insertNodes = insertElement(topRec, txn);
		insertBranch(topRec,insertNodes);
		topRec.setHead(insertNodes.get(insertNodes.size()-1).getEltId());
		database.put(null, key, topRecMapper.toRecord(topRec));
		
		// delete (if exists) the overflow record lock
		if (this.database.exists(null, scKey)) {
			this.database.delete(null, scKey);
		}
		
	}

	/*
	 * Insert a new data node,
	 * If the record does not exist
	 * 		create the record
	 * insert the data node
	 */
	
	public void insertNewDataNode(String uKey, double amt) {
		this.setUserKey(uKey);
		
		TopRecord topRec = null;
		TopRecordMapper topRecMapper = new TopRecordMapper();
		Key key = new Key(nameSpace, "toprecords", uKey);
		Key scKey = new Key(lockNameSpace, "lockset", uKey);
		Record record = database.get(null, key);
		if (record == null) {	// record does not exist, so create
			topRec = new TopRecord( database, nameSpace);
			topRec.setUserKey(uKey);
			this.userKey = uKey;
			topRec.setDateOpened(new Date());
			topRec.setNameOnAccount(topRec.getUserKey());
		}
		else {
			
			topRec = topRecMapper.fromRecord(record);
			topRec.setDatabase(database);
			topRec.setNamespace(nameSpace);
		}
		
		
		topRec.setLockNameSpace(lockNameSpace);
		ArrayList <DataNode> insertNodes;
		DataNode dNode;
		String eltId;
		Date currentDate = new Date();
		
		eltId = currentDate.toString();								
		dNode = new DataNode(eltId, (float)0, currentDate);
		dNode.setEltValue(amt);
		
		insertNodes = insertElement(topRec, dNode);
		insertBranch(topRec, insertNodes);
		topRec.setHead(insertNodes.get(insertNodes.size()-1).getEltId());
		database.put(null, key, topRecMapper.toRecord(topRec));
		
		// delete (if exists) the overflow record lock
		removeSCLock();
		
	}
	
	
	
	/*
	 * return the specified element or null if the record does not exist
	 *  the eltDate needs to be a string in the form "yyyyMMddhhmmss"
	 */
	public DataNode retreiveElement( String uKey, String eltId, String eltDate ){
		Key key = new Key(nameSpace, "toprecords", uKey);
		Record record = database.get(null, key);
		if ( record == null ) {
			return( null );
		}
		TopRecordMapper topRecMapper = new TopRecordMapper();
		TopRecord topRec = topRecMapper.fromRecord(record);
		topRec.setDatabase(database);
		topRec.setNamespace(nameSpace);
		
		DataNode dNode = topRec.getElement(eltId, eltDate);
		
		return (dNode);
	}

	/*
	 * used to insert a new element
	 */
	public ArrayList<DataNode> insertElement(TopRecord topRec, DataNode newElt) {
		ArrayList <DataNode> insertNodeList = new ArrayList<DataNode>();
		
		checkSCLock(userKey);
		// first node
		if (topRec.getDataGraph().isEmpty()) {
			
			topRec.initializeMappers (newElt);
		}
		insertNodeList.add(newElt);	
			
		return insertNodeList;	
		
	}
	
	/*
	 * calculate the space needed for the new element list and create an overflow SubRecord if needed
	 * insertNodes: the nodes are in insertion order. I.E. the first node in the list is the farthest back in time
	 * 
	 * What happens if the new branch will exceed the the RECORD_SIZE, this is currently not handled
	 */
	public boolean insertBranch(TopRecord topRec, ArrayList<DataNode> insertNodes) {
		int numNewNodes = insertNodes.size();
		boolean didOverflow = false;
		//long numBytesTxn = ObjectSizeFetcher.getObjectSize(insertNodes.get(0));
		int sizeOfMappers = MAPPERS_SIZE * topRec.getEltIdMapper().size();
		long currObjSz = SIZEOF_ELEMENT_NODE * topRec.getDataGraph().size() + sizeOfMappers + T_RECORD_SIZE;
		
		int additionalSize = numNewNodes * SIZEOF_ELEMENT_NODE;
		
		Iterator itr = insertNodes.iterator();
		DataNode curNode;
		
		if (currObjSz + additionalSize > RECORD_SIZE * BLOCK_MULTIPLIER){ // overfloww
			/*
			 * create an entry in the strong consistency namespace
			 * 
			 * create a SUBRECORD with the right naming
			 * copy dataGraph into SubRecord
			 * put SubRecord
			 *
			 * delete contents of dataGraph
			 * insert new node list
			 * update the mappers in topRecord
			 */
			//System.out.println("in Overflow");
			
			didOverflow = true;
			createSCLock();
			
			SubRecord subRec = new SubRecord(topRec.getDataGraph(), topRec.getHead());
			String newUserKey = topRec.getEltIdMapper().get(topRec.getEltIdMapper().size()-1).get(0);  
			topRec.incrementNumOverflow();

			
			if (newUserKey.contains("-") ){
				String []split = newUserKey.split("-");
				int suffix =Integer.parseInt(split[1]) + 1;
				newUserKey = split[0] + "-" + suffix;
			
				
			}
			else {		// first subRecord
				newUserKey += "-1";
			}
			Key key = new Key(nameSpace, "subrecords", newUserKey);
			
			this.database.put(null, key, mapper.toRecord(subRec));
			
			topRec.getDataGraph().clear();
			topRec.updateOverFlowMappers(newUserKey);
			topRec.initializeMappers(insertNodes.get(0));
			insertNodes.get(0).setPreviousPaths(topRec.getHead()); 		// set the previous points
			
			
		}
		// insert the insertnodes into the TopRecord dataGraph
		// insertNodes: the nodes are in insertion order. I.E. the first node in the list is the farthest back in time
		while (itr.hasNext()){
			curNode = (DataNode) itr.next();
			topRec.getDataGraph().put(curNode.getEltId(), curNode);
		}
			
		
		topRec.updateMappers( insertNodes.get(numNewNodes -1) );
		
		return didOverflow;
		
		
	}
	
	public void createSCLock() {
		Key scKey = new Key(lockNameSpace, "lockset", userKey);
		Bin scBin = new Bin("lock", 1);
		
		try {
			this.database.put(null, scKey, scBin);
		} catch (AerospikeException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
	
	public void removeSCLock() {
		Key scKey = new Key(lockNameSpace, "lockset", userKey);
		if (this.database.exists(null, scKey)) {
			this.database.delete(null, scKey);
		}
	}
	
	public int getBLOCK_MULTIPLIER() {
		return BLOCK_MULTIPLIER;
	}

	// record size is limited to 1 M, 8 * 128K = 1 M, therefore BLOCK_MULTIPLIER cannot exceed 8
	public void setBLOCK_MULTIPLIER(int bLOCK_MULTIPLIER) {
		BLOCK_MULTIPLIER = (bLOCK_MULTIPLIER > 8 ? 8 : bLOCK_MULTIPLIER);
	}

	private void checkSCLock(String uKey) {
		Key scKey = new Key(lockNameSpace, "lockset", uKey);
		boolean status;
		try {
			status = this.database.exists(null, scKey);
		} catch (AerospikeException ae){
			System.out.println(ae.getMessage());
			System.out.println("ns: " + lockNameSpace);
		}
		
		while (this.database.exists(null, scKey)) {
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
	}
}
