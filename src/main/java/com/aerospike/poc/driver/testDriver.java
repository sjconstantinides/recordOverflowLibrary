package com.aerospike.poc.driver;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.poc.recordOverflow.DataAccessMethods;
import com.aerospike.poc.recordOverflow.mappers.TopRecordMapper;
import com.aerospike.poc.recordOverflow.model.DataNode;
import com.aerospike.poc.recordOverflow.model.TopRecord;

public class testDriver {
	private static DataAccessMethods dam;
	private static final int numberOfDataElements = 100;
	private static final int numberOfRecords = 10;
	private static final String recordId = new String ("testRecord");
	
	
	public testDriver() {
		super();
		
	}
	private static void printUsage(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		String syntax = "testDriver" + " [<options>]";
		formatter.printHelp(pw, 100, syntax, "options:", options, 0, 2, null);
		System.out.println(sw.toString());
	}

	
	public static void main(String[] args) {
		IAerospikeClient database = null;
	
		
		try {
			Options options = new Options();
			options.addOption("h", "host", true, "Server hostname (default: 127.0.0.1)");
			options.addOption("p", "port", true, "Server port (default: 3000)");
			options.addOption("n", "namespace", true, "Namespace (default: test)");
			options.addOption("s", "scnamespace",  true, "Strong Consistency Namespace (default: lock_ns)");
			options.addOption("r", "reset", true, "Reset the database before run");
			
	
			options.addOption("u", "usage", false, "Print usage.");
			
			CommandLineParser parser = new PosixParser();
			CommandLine cl = parser.parse(options, args, false);
			
			if (cl.hasOption("u")){
				printUsage(options);
				System.exit(1);
			}

			String rOption = cl.getOptionValue("reset", "false");
			boolean reset = Boolean.valueOf(rOption);


			String host = cl.getOptionValue("h", "127.0.0.1");
			String portString = cl.getOptionValue("p", "3000");
			int port = Integer.parseInt(portString);
			String namespace = cl.getOptionValue("namespace", "test");
			String lockNameSpace = cl.getOptionValue ("scnamespace", "lock_ns");				
			
			
			long randomSeed = Long.valueOf(cl.getOptionValue("random", "0"));

		   
			Random random = new Random(randomSeed);
			
			if (database == null ) {

				ClientPolicy cp = new ClientPolicy();
				cp.useServicesAlternate = true;
				database = new AerospikeClient(cp, host, port);
				
			}
			if (reset) {
				database.truncate(null, namespace, "toprecords", null);
				database.truncate(null, namespace, "subrecords", null);
				database.truncate(null, lockNameSpace, "lockset", null);

				database.truncate(null,namespace, "customers", null);
			}
			
			System.out.println("host: " + host);
			System.out.println("port: " + portString);
			System.out.println("namespace: " + namespace);
			System.out.println("sc: " + lockNameSpace);


			dam = new DataAccessMethods(database, namespace, lockNameSpace);
			TopRecordMapper mapper = new TopRecordMapper();
			
			String uKey;
			// create one node in each record to initialize everything
			for (int i = 0; i < numberOfRecords; i++ ){
				uKey = new String(recordId + i);
				dam.insertNewDataNode(uKey, (float)random.nextInt(numberOfDataElements));
			}
			
			// create a random number of elements for each record
			int numElts; 
			ArrayList<DataNode> nodeList = new ArrayList<DataNode>();
			TopRecord topRec;
			DataNode dNode;
			Date curDate = new Date();
			String eltId;
			Key key;
			Record record;
			for (int i = 0; i < numberOfRecords; i++){
				uKey = new String(recordId + i);
				key = new Key(namespace, "toprecords", uKey);
				record = database.get(null, key);
				topRec = mapper.fromRecord(record);
				
				numElts = random.nextInt(numberOfDataElements) + 1;
				for (int j = 0; j < numElts; j++ ) {
					
					curDate = new Date();
					
					eltId = curDate.toString();								
					dNode = new DataNode(eltId, (float)random.nextInt(numberOfDataElements), curDate);
					nodeList.add(dNode);
					
				}
				
				dam.insertBranch(topRec, nodeList);
				key = new Key(namespace, "toprecords", topRec.getUserKey());
				database.put(null, key, mapper.toRecord(topRec));
				nodeList.clear();
				
			}
			
		


		} catch (Exception e) {
			System.out.println("Critical error: " + e.getMessage());
		}
		finally {
			System.out.println("Shutting down program");
			if (database != null) {
				database.close();
			}
		}
	}

	
	
}
