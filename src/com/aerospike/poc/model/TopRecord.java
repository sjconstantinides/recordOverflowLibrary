package com.aerospike.poc.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.aerospike.client.Bin;
import com.aerospike.client.Record;
import com.aerospike.client.Value;

import java.util.TreeMap;

import com.aerospike.client.Value.MapValue;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapWriteMode;





/**
 * Customers have accounts, accounts have cards associated with them
 * @author Tim, modified by Stefan
 *
 */
public class TopRecord {

	private String number;
	private String nameOnAccount;
	private Date dateOpened;
	
	private String accountId;			// SJC 
	private ArrayList<ArrayList<String>> txnIdMapper;
	private ArrayList<ArrayList<String>> effMapper;
	private ArrayList<ArrayList<String>> arrMapper;
	private Map<String,TransactionNode> transactionGraph;
	private String head;		// key to the head TransactionNode of the graph
	private int compensatingNodeIdx;		// only used for testing
	
	private final String binName = "txnGraph";
	private MapOperation mapOpp = new MapOperation();
	private MapPolicy mapPolicy;
	private Bin tGraph;
	
	public int getCompensatingNodeIdx() {
		return compensatingNodeIdx;
	}

	public void setCompensatingNodeIdx(int compensatingNodeIdx) {
		this.compensatingNodeIdx = compensatingNodeIdx;
	}

	public TopRecord(){
		txnIdMapper = new ArrayList< ArrayList< String>>();
		effMapper = new ArrayList< ArrayList< String >>();
		arrMapper = new ArrayList< ArrayList< String >>();
		transactionGraph = new HashMap<String, TransactionNode>();
		head = null;
		compensatingNodeIdx = 1;
		number = new String("0");
		tGraph = new Bin(binName, transactionGraph);
		mapPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE);
		mapOpp.setMapPolicy(mapPolicy, binName);
	}
	
	public String getNumber() {
		return number;
	}
	public void setNumber(String number) {
		this.number = number;
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
	public ArrayList<ArrayList<String>> getEffMapper() {
		return effMapper;
	}
	public void setEffMapper(ArrayList<ArrayList<String>> effMapper) {
		this.effMapper = effMapper;
	}
	public ArrayList<ArrayList<String>> getArrMapper() {
		return arrMapper;
	}
	public void setArrMapper(ArrayList<ArrayList<String>> arrMapper) {
		this.arrMapper = arrMapper;
	}
	public Map<String,TransactionNode> getTransactionGraph() {
		return transactionGraph;
	}
	public void setTransactionGraph(Map<String, TransactionNode> map) {
		this.transactionGraph = map;
	}
	public String getHead() {
		return head;
	}
	public void setHead(String head) {
		this.head = head;
	}

	/*
	 * This will only be called during the POC and for testing, The logic will probably be duplicated in the real code
	 */
	public ArrayList<TransactionNode> insertTransaction(TransactionNode newTxn) {
		int numNodesAdded = 1;
		ArrayList <TransactionNode> insertNodeList = new ArrayList<TransactionNode>();
		// first node
		if (transactionGraph.isEmpty()) {
			
			//mapOpp.putItems(mapPolicy, binName, Value.MapValue(transactionGraph));
			//transactionGraph.put(newTxn.getTxnId(), newTxn);
			insertNodeList.add(newTxn);
			head = newTxn.getTxnId();		// TODO: should move this outside to handle the overflow
			initializeMappers (newTxn);
		}
		else {
			
			// check for branch or not
			TransactionNode curNode = transactionGraph.get(head);
			boolean isBranch = false, endOfGraph = false;
			String prevSearchKey = curNode.getPreviousRealTxnId();
			ArrayList <TransactionNode> updateList = new ArrayList<TransactionNode>();
			
			// cycle through to find where in time to insert
			while ( !endOfGraph && curNode != null && curNode.isOlder(newTxn.getEffectiveDate())) {
				isBranch = true;
				updateList.add(curNode);
				
				if (prevSearchKey == null){
					endOfGraph = true;
				}
				else {
					prevSearchKey = curNode.getTxnId();
					curNode = transactionGraph.get(curNode.getPreviousIdealTxnId());
				}
				
			}
			if (!isBranch){			// simple insert and new head
				
				newTxn.setPreviousPaths(curNode.getTxnId());
				newTxn.setBalance(curNode.getBalance() + newTxn.getTxnAmt());
				head = newTxn.getTxnId();		// TODO: should move this outside of insert incase we need to overflow
				newTxn.printData();
				updateMappers(newTxn);
				//transactionGraph.put(newTxn.getTxnId(), newTxn);
				insertNodeList.add(newTxn);
				
			}
			else {
				int numNode = transactionGraph.size();
				createBranch( updateList, curNode, newTxn, insertNodeList );	//TODO: set head outside
				numNodesAdded = transactionGraph.size() - numNodes;
			}
			
			
		}
		return insertNodeList;
		
		
	}

	private void initializeMappers(TransactionNode newTxn) {
		ArrayList<String> insertList = new ArrayList<String>();
		insertList.add(accountId);
		
		insertList.add(newTxn.getTxnId());
		txnIdMapper.add(insertList);
		
		insertList.remove(1);
		insertList.add(Long.toString(newTxn.getArrivalDate()));
		arrMapper.add(insertList);
		
		insertList.remove(1);
		insertList.add(Long.toString(newTxn.getEffectiveDate()));
		arrMapper.add(insertList);
		
		
	}

	/*
	 * used primarily for creation, in real application might be able to do this with a mapOperation command
	 * or a better way to replace a value in a list
	 */
	private void updateMappers(TransactionNode newTxn) {
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
		arrMapper.add(insertList);
		
		insertList = effMapper.get(0);
		if (insertList.size() >= 3){
			insertList.remove(2);
		}
		insertList.add(2, Long.toString(newTxn.getArrivalDate()));
		effMapper.remove(0);
		effMapper.add(insertList);
		
	}

	/*
	 * This will only be used by the testing program, the logic will be reused 
	 * it is assumed that new transactions will only be created by the Capital One 
	 * team, new transactions are created here for test purposes
	 */
	private String createBranch(ArrayList<TransactionNode> updateList, TransactionNode insertAfter, TransactionNode newTxn, ArrayList<TransactionNode> insertNodeList) {
		int i = updateList.size() -1;
		TransactionNode cloneNode = null, compNode;
		
		// set the needed values in the new transaction node
		
		newTxn.setPreviousRealTxnId(head);
		newTxn.setPreviousIdealTxnId( (insertAfter == null ? null : insertAfter.getTxnId()) );
		newTxn.setBalance((insertAfter == null ? newTxn.getTxnAmt() : insertAfter.getBalance() + newTxn.getTxnAmt()));
		//transactionGraph.put(newTxn.getTxnId(), newTxn);
		insertNodeList.add(newTxn);
		
		String prevId = newTxn.getTxnId();
		
		
		for (; i>= 0; i--) {
			cloneNode = new TransactionNode(updateList.get(i));
			cloneNode.setPreviousPaths(prevId);
			cloneNode.setBalance(transactionGraph.get(prevId).getBalance() + cloneNode.getTxnAmt());
			//transactionGraph.put(cloneNode.getTxnId(), cloneNode);
			insertNodeList.add(cloneNode);
			
			prevId = cloneNode.getTxnId();
			cloneNode.printData();
		}
		
		// add in the compensating node
		compNode = new TransactionNode();
		compNode.setTxnId(new String("comp" + this.compensatingNodeIdx++));
		compNode.setBalance(cloneNode.getBalance());
		compNode.setArrivalDate(newTxn.getArrivalDate());
		compNode.setEffectiveDate(newTxn.getArrivalDate() );
		compNode.setHasBranch(true);
		compNode.setPreviousRealTxnId(head);
		compNode.setPreviousIdealTxnId(prevId);
		
		compNode.printData();
		updateMappers (compNode);
		
		//transactionGraph.put(compNode.getTxnId(), compNode);
		insertNodeList.add(compNode);
		
		return compNode.getTxnId();
		
	}
}
