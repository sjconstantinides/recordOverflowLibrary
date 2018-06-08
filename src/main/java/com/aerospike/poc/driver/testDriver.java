package com.aerospike.poc.driver;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.poc.recordOverflow.DataAccessMethods;

public class testDriver {
	private static DataAccessMethods dam;
	private final int numberOfDataElements = 100;
	private final int numberOfRecords = 250;
	
	
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
			
	
			options.addOption("u", "usage", false, "Print usage.");
			
			CommandLineParser parser = new PosixParser();
			CommandLine cl = parser.parse(options, args, false);


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


			dam = new DataAccessMethods(database, namespace, lockNameSpace);
			
			// TODO: add in test driver calls 


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
