package com.aerospike.poc.recordOverflow.model;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DataNode {
	
	private String txnId;
	private double balance;
	private double txnAmt;
	private long arrivalDate;
	private String arrivalTime;
	
	private String previousTxnId;			// traverse by arrival time  
	
	
	public DataNode(String inTxnId, float inBal, Date arrDate ){
		txnId = inTxnId;
		balance = inBal;
		txnAmt = 0;
		
		SimpleDateFormat sdfDate = new SimpleDateFormat("yyyyMMdd");
		SimpleDateFormat sdfTime = new SimpleDateFormat("hhmmss");
		
		
		arrivalTime = sdfDate.format(arrDate) + sdfTime.format(arrDate);
		arrivalDate = Long.parseLong(arrivalTime);

		this.setPreviousTxnId (null);
	}

	// constructor to clone, but supply the previous node ID
	public DataNode(DataNode origNode) {
		this.txnId = origNode.getTxnId() + "'";		// adding ' to the txnID to make it a unique ID.
		
		this.balance = origNode.getBalance();
		this.txnAmt = origNode.getTxnAmt();
		this.arrivalDate = origNode.getArrivalDate();
		this.arrivalTime = origNode.getArrivalTime();	
	}

	public DataNode() {
		// TODO Auto-generated constructor stub
	}

	public void create() {
		this.setTxnId("DBTRAN25");
		this.setBalance((float)257.30);
		this.setTxnAmt((float)35.70);
		this.setArrivalTime("20170919");

	}


	public long valueToSortBy() {
		return arrivalDate;
	}

	public String getTxnId() {
		return txnId;
	}

	public void setTxnId(String txnId) {
		this.txnId = txnId;
	}

	public double getBalance() {
		return balance;
	}

	public void setBalance(double balance) {
		this.balance = balance;
	}

	public double getTxnAmt() {
		return txnAmt;
	}

	public void setTxnAmt(double txnAmt) {
		this.txnAmt = txnAmt;
	}

	public String getArrivalTime() {
		return arrivalTime;
	}

	public void setArrivalTime(String arrivalTime) {
		this.arrivalTime = arrivalTime;
	}

	public long getArrivalDate() {
		return arrivalDate;
	}

	public void setArrivalDate(long arrivalDate) {
		this.arrivalDate = arrivalDate;
	}

	public String getPreviousTxnId() {
		return previousTxnId;
	}

	public void setPreviousTxnId(String previousTxnId) {
		this.previousTxnId = previousTxnId;
	}

	public void setPreviousPaths(String txnId) {
		this.setPreviousTxnId(txnId);
		
	}

	public void printData() {
		System.out.println("\nContents of Txn ID: " + txnId);
		System.out.println("\t Balance: " + balance);
		System.out.println("\t txnAmt: " + txnAmt);
		System.out.println("\t arrival date: " +  arrivalDate);
	
		
	}
}
