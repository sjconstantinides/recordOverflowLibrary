package com.aerospike.poc.recordOverflow.model;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DataNode {
	
	private String eltId;
	private double sum;
	private double eltValue;
	private long arrivalDate;
	private String arrivalTime;
	
	private String previousEltId;			// traverse by arrival time  
	
	
	public DataNode(String inEltId, float inVal, Date arrDate ){
		eltId = inEltId;
		sum = inVal;
		eltValue = 0;
		
		SimpleDateFormat sdfDate = new SimpleDateFormat("yyyyMMdd");
		SimpleDateFormat sdfTime = new SimpleDateFormat("hhmmss");
		
		
		arrivalTime = sdfDate.format(arrDate) + sdfTime.format(arrDate);
		arrivalDate = Long.parseLong(arrivalTime);

		this.setPreviousEltId (null);
	}

	// constructor to clone, but supply the previous node ID
	public DataNode(DataNode origNode) {
		this.eltId = origNode.getEltId() + "'";		// adding ' to the txnID to make it a unique ID.
		
		this.sum = origNode.getSum();
		this.eltValue = origNode.getEltValue();
		this.arrivalDate = origNode.getArrivalDate();
		this.arrivalTime = origNode.getArrivalTime();	
	}

	public DataNode() {
		// TODO Auto-generated constructor stub
	}

	public void create() {
		this.setEltId("DBTRAN25");
		this.setSum((float)257.30);
		this.setEltValue((float)35.70);
		this.setArrivalTime("20170919");

	}


	public long valueToSortBy() {
		return arrivalDate;
	}

	public String getEltId() {
		return eltId;
	}

	public void setEltId(String eltId) {
		this.eltId = eltId;
	}

	public double getSum() {
		return sum;
	}

	public void setSum(double inSum) {
		this.sum = inSum;
	}

	public double getEltValue() {
		return eltValue;
	}

	public void setEltValue(double eltVal) {
		this.eltValue = eltVal;
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

	public String getPreviousEltId() {
		return previousEltId;
	}

	public void setPreviousEltId(String prevEltId) {
		this.previousEltId = prevEltId;
	}

	public void setPreviousPaths(String txnId) {
		this.setPreviousEltId(txnId);
		
	}

	public void printData() {
		System.out.println("\nContents of Txn ID: " + eltId);
		System.out.println("\t Sum: " + sum);
		System.out.println("\t element value: " + eltValue);
		System.out.println("\t arrival date: " +  arrivalDate);
	
		
	}
}
