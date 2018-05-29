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
	private String nameOnAccount;
	private Date dateOpened;

	
	private String accountId;			// SJC 
	private ArrayList<ArrayList<String>> txnIdMapper;
	private ArrayList<ArrayList<String>> arrMapper;
	private Map<String,DataNode> dataGraph;
	private String head;		// key to the head TransactionNode of the graph
	private int compensatingNodeIdx;		// only used for testing
	
	private final String binName = "txnGraph";
	private MapOperation mapOpp = new MapOperation();
	private MapPolicy mapPolicy;
	private Bin tGraph;
	
	private final SubRecordMapper mapper = new SubRecordMapper();
	private IAerospikeClient database;
	private String keySpace;
	private String lockNameSpace;
	
	public int getCompensatingNodeIdx() {
		return compensatingNodeIdx;
	}

	public void setCompensatingNodeIdx(int compensatingNodeIdx) {
		this.compensatingNodeIdx = compensatingNodeIdx;
	}

	public TopRecord(){
		txnIdMapper = new ArrayList< ArrayList< String>>();
		arrMapper = new ArrayList< ArrayList< String >>();
		dataGraph = new HashMap<String, DataNode>();
		head = null;
		compensatingNodeIdx = 1;
		numOverflow = 0;
		tGraph = new Bin(binName, dataGraph);
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
	public String getNameOnAccount() {
		return nameOnAccount;
	}
	public void setNameOnAccount(String nameOnAccount) {
		this.nameOnAccount = nameOnAccount;
	}
	public String getAccountId() {
		return accountId;
	}
	public void setAccountId(String inActId) {
		this.accountId = inActId;
	}
	public Date getDateOpened() {
		return dateOpened;
	}
	public void setDateOpened(Date dateOpened) {
		this.dateOpened = dateOpened;
	}
	public ArrayList<ArrayList<String>> getTxnIdMapper() {
		return txnIdMapper;
	}
	public void setTxnIdMapper(ArrayList<ArrayList<String>> txnIdMapper) {
		this.txnIdMapper = txnIdMapper;
	}
	
	public ArrayList<ArrayList<String>> getArrMapper() {
		return arrMapper;
	}
	public void setArrMapper(ArrayList<ArrayList<String>> arrMapper) {
		this.arrMapper = arrMapper;
	}
	public Map<String,DataNode> getTransactionGraph() {
		return dataGraph;
	}
	public void setTransactionGraph(Map<String, DataNode> map) {
		this.dataGraph = map;
	}
	public String getHead() {
		return head;
	}
	public void setHead(String head) {
		this.head = head;
	}





	public void initializeMappers(DataNode newTxn) {
		ArrayList<String> insertList = new ArrayList<String>();
		insertList.add(accountId);
		
		insertList.add(newTxn.getTxnId());
		txnIdMapper.add(0,insertList);
		
		insertList.remove(1);
		insertList.add(Long.toString(newTxn.getArrivalDate()));
		arrMapper.add(0,insertList);
		
		
	}

	/*
	 * used primarily for creation, in real application might be able to do this with a mapOperation command
	 * or a better way to replace a value in a list
	 * 
	 * the mapper lists contain only two elements: the first key and the last key, 
	 * Use txnIdMapper if the transaction IDs are sequential, Use arrMapper otherwise
	 */
	public void updateMappers(DataNode newTxn) {
		ArrayList<String> insertList = txnIdMapper.get(0);
		
		if (insertList.size() >= 3){
			insertList.remove(2);
		}
		insertList.add(2, newTxn.getTxnId());
		txnIdMapper.remove(0);
		txnIdMapper.add(0, insertList);
		
		insertList = arrMapper.get(0);
		if (insertList.size() >= 3){
			insertList.remove(2);
		}
		insertList.add(2, Long.toString(newTxn.getArrivalDate()));
		arrMapper.remove(0);
		arrMapper.add(0, insertList);
		
		
	}






	public void updateOverFlowMappers(String newAcctId) {
		
		txnIdMapper.get(0).remove(0);
		txnIdMapper.get(0).add(0,newAcctId);
		
		arrMapper.get(0).remove(0);
		arrMapper.get(0).add(0,newAcctId);
			
	}

	public void setDatabase(IAerospikeClient database2) {
		this.database = database2;
		
	}

	public void setNamespace(String nameSpace) {
		this.keySpace = nameSpace;
		
	}

	/*
	 * returns the found transaction node or null if not found
	 */
	public DataNode getTransaction(String txnId, String txnDate) {
		DataNode txn = dataGraph.get(txnId);
		if (txn != null){		// no overflow so in topRecord
			return ( txn);
		}
		TopRecordMapper topRecMapper = new TopRecordMapper();
		int subRecNum = findSubRecord( arrMapper, txnId, txnDate);	// use the arrival time Mapper
		if (subRecNum > 0){
			String subRecId = new String(accountId + "-" + subRecNum);
			Key key = new Key(keySpace, "subrecords", subRecId);
			Record record = database.get(null, key);
			Map<String,DataNode> localTxnGraph = topRecMapper.buildGraphFromDB(record);
			return( localTxnGraph.get(txnId));
		}
		
		return null;	
		
	}

	public int findSubRecord(ArrayList<ArrayList<String>> mapper, String txnId, String txnDate) {
		int subRecIdx = 0;
		boolean found = false;
		Iterator<ArrayList<String>> curList = mapper.iterator();
		ArrayList<String> curSubList;
		int txnDateNum = Integer.parseInt(txnDate);
		
		while (curList.hasNext() && !found) {
			curSubList = curList.next();
			if ( txnDateNum >= Integer.parseInt(curSubList.get(0)) && txnDateNum <= Integer.parseInt(curSubList.get(1))){
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
