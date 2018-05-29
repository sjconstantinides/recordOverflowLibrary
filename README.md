# Realtime analytics with Aerospike

This package is a self contained example of how to handle record overflow for Aerospike records
```
The basic components of the sample:
	- DataAccessMethods.java is the main entry point to the record overflow constructs. 
		Currently available access methods:
		- insertNewTxn( key, txnAmt)
			where key is a unique String identifyer to associate an Aerospike record to be stored.
			txnAmt is a floating point number to be inserted.
			The current timestamp is associated with the inserted data.
		- retrieveRecentTransactions( key, numTrans )
			where key again is the unique String identifyer
			numTrans is the number of transactions to return going back in time. 

	Variables are used to calculate the current size of the record and the additional space needed.
	Those variables are currently hard coded to specific values to force record overflow to be able to test. 
	Those variables are defined at the top of the DataAccessMethods.java file, you can change those as needed
		Currently these are set to overflow at 131,072 bytes or 128 KB (RECORD_SIZE).
		Also to force overflow the base transaction size is set to 1500 bytes (SIZEOF_TRANS_NODE)

	An idea for future enhancements would be to put these values into some .properties file and read them in on startup. This would also allow for you to tune your application to balance between record size, and retrieval time.

	- The actual structures that contain the transaction data and the meta data structures
		DataNode: class containing the data used to store and traverse the data graph structure
		TopRecord: class containing the transaction graph, and mapper(table of contents) information
		SubRecord: overflow class structure to hold the data that overflows the current record
	- There are Mapper classes contained in the package to map the objects to be stored in Aerospike.
	- There is a JUnit test driver included: TestInsertTransaction.java
		
```
		


## Disclaimer
This is not production code, this is sample code that demostrates how to handle growth when the object to be stored exceeds the Aerospike size limitation.

## Handling Record Overflow
One of the key components used to handle record overflow is taking advantage of Strong Consistency within Aerospike. Since a record overflow involves creating and updating a sub-record or an overflow record, multiple records will need to be updated at one time. This is not an issue if only one source can update one particular unique key, However to handle the general case we have added a strong consistency "lock_ns" namespace. (please see the Aerospike documentation to properly configure a "strong consistency" namespace: https://www.aerospike.com/docs/operations/manage/consistency/index.html
As soon as there is an overflow condition an scKey is created and a simple integer value is created and stored in the lock_ns. This is removed after the top-record and the corresponding sub-record has been updated.

below is an example of how to define an Aerospike namespace with strong consistency

```
namespace lock_ns {
	replication-factor 2
	memory-size 1G
	default-ttl 0 
	strong-consistency true
	strong-consistency-allow-expunge true

	#	storage-engine memory

	# To use file storage backing, comment out the line above and use the
	# following lines instead.
	storage-engine device {
		file /opt/aerospike/data/lock_ns.dat
		filesize 4G
		data-in-memory true # Store data in memory in addition to file.
	}
}

```
		
### Test data
The test data can be generated to simulate a real data set.
The assumption is that in an actual transaction system, the transaction IDs will be created externally to this code and sent in. For test purposes, the creation date of the transaction is used as the transaction ID



#### Running the code

```
	- You will need to include the recordOverflow package into your project description and into your project hierarchy
	- TestInsertTransaction.java contains the basic information to connect to an Aerospike database and can be run via standard JUnit commands. You can use the code within TestInsertTransaction.java as a guide for your own code.
	- The needed components are:
		IP address of your Aerospike server
		name of the basic namespace
		name of the strong consistency namespace
	  Currently these are hard coded into the TestInsertTransaction.java and will need to be changed to match your environment. These are typically set via run time command line arguments. (see the Aerospike examples for this specifically)
	  The AerospikeClient reference, and both namespaces are used in the constructor call to the DataAccessMethods class.

	
```

#### Output

Below is a sample output from AQL showing the number of toprecords and the overflow into the subrecords

```
aql> show sets
+------------------+-----------+----------------+---------+-------------------+--------------+-------------------+----------------+------------+
| disable-eviction | ns        | set-enable-xdr | objects | stop-writes-count | set          | memory_data_bytes | truncate_lut   | tombstones |
+------------------+-----------+----------------+---------+-------------------+--------------+-------------------+----------------+------------+
| "false"          | "test"    | "use-default"  | "100"   | "0"               | "toprecords" | "219561"          | "265319093981" | "0"        |
| "false"          | "test"    | "use-default"  | "100"   | "0"               | "subrecords" | "1207300"         | "0"            | "0"        |
| "false"          | "lock_ns" | "use-default"  | "0"     | "0"               | "lockset"    | "0"               | "265319093989" | "0"        |
+------------------+-----------+----------------+---------+-------------------+--------------+-------------------+----------------+------------+
```

Below is a portion of the contents of the mapping information contained in the TopRecord indicating overflow to a subrecord

```
aql> select arrMapper from test
...
+------------------------------------------------------------------------------------------------------------------+
| arrMapper                                                                                                        |
+------------------------------------------------------------------------------------------------------------------+
| LIST('[["202751322", "20180503051111", "20180712025406"], ["202751322-1", "20161127090122", "20180428093007"]]') |
| LIST('[["202752069", "20180502021904", "20180712094344"], ["202752069-1", "20161126041444", "20180429010012"]]') |
+------------------------------------------------------------------------------------------------------------------+
...
```

This is a representation of the ArrayList<ArrayList<String>> within TopRecord. The inner List is defined as follows:
```
	Element 0: record key (user key, not Aerospike Key)
	Element 1: starting date of all the transaction contained in this record
	Element 2: ending date of all the transactions contained in this record
```

Notice the '-1' as the record key for the second tuple. This indicates a subrecord was created with the '-1' extention. As more overflow records are created the '-n' will increase. The larger the 'n': the more current the data is. The smaller the 'n': the farther back in time you go.

## Discussion 

The basic premise of the framework is to have a "top record" that the end user points to via their user defined unique key. The user's top level record usually contains some map or list structure that can continue to grow. As the record grows, eventually it will grow to large for the Aerospike defined record size constraints. Before it gets to that point this framework will determine that the record is too big and create a "sub record", copy the contents into the sub record and then reset the current record to its initial state.

The top record in the framework also contains some mapping information analoguous to a table of contents to indicate what range of data is contained in the top record and all the sub records. This informaiton is only contained in the top record and not in the sub records.
Note: these "mapper" fields are not to be confused with the "Mappers" used to store the data into Aerospike.

As mentioned above the user client will only ever know about, and have a Key to the top record. But this framework allows for continued data growth and avoid the "Record too big" error.


 
