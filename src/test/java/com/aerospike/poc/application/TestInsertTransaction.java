package com.aerospike.poc.application;

import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.poc.recordOverflow.DataAccessMethods;
import com.aerospike.poc.recordOverflow.model.DataNode;

public class TestInsertTransaction {
	private DataAccessMethods dam;
	private AerospikeClient asClient;
	private static final String namespace = "test";
	private static final String lockNS = "lock_ns";
	

	@Before
	public void setUp() throws Exception {
		
		asClient = new AerospikeClient("172.28.128.3", 3000);
		dam = new DataAccessMethods(asClient, namespace, lockNS);
	}

	@After
	public void tearDown() throws Exception {
		asClient.close();
	}

	@Test
	public void testInvalidTransaction() {
	
		try {	
			dam.insertNewTxn("doesntExist", 10.0);
			fail("Added value to invalid transaction");
		}
		catch (Exception e) {
			
		}
	}
	
	@Test
	public void testValidTransaction() {
		DataNode txn;
		double balance;
		txn = dam.retrieveHead("202751241");
		balance = txn.getBalance();
		dam.insertNewTxn("202751241", 35.0);
		txn = dam.retrieveHead("202751241");
	
		assert(txn.getBalance() == balance + 35.0);
	}
	
	@Test
	public void testOverflow() {
		int i, numTransToAdd = 100;
		for (i = 0; i < numTransToAdd; i++) {
			testValidTransaction();
		}
	}
	

}
