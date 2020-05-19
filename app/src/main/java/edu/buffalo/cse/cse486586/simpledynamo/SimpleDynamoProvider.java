package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Array;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.opengl.Matrix;
import android.os.AsyncTask;
import android.os.Handler;
import android.os.Looper;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.widget.Toast;

public class SimpleDynamoProvider extends ContentProvider {

	static String RECORDS_FILE = "RECORDS_FILE";
	static String SELECTION_ALL = "*";
	static String SELECTION_LOCAL = "@";
	static String SELECTION_LOCAL_WITH_TIMESTAMP = "@*";

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		if (selection.equals(SELECTION_ALL)){
			DeleteAllGlobally deleteGlobally = new DeleteAllGlobally();
			deleteGlobally.start();
			try {
				deleteGlobally.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}else if (selection.equals(SELECTION_LOCAL)){

			// the delete local records phase consists 2 steps.
			// We do not want any operation to occur in the middle
			synchronized (SimpleDynamoProvider.class) {
				deleteAllLocalRecords();
				initializeRecordsFile();
			}

		}else {
			/*
			Protocol:
			selection - key;Constants.ADDRESS_SELF
			Constants.ADDRESS_SELF will not exist if the query is passed on my a local node
			 */
			String[] splitSelection = selection.split(";");
			if (splitSelection.length == 2){
				synchronized (SimpleDynamoProvider.class) {
					deleteLocalKey(splitSelection[0]);
				}
				return 1;
			}
			Node coordinator = GetCoordinatorForKey(selection);
			try {
				// delete the key from coordinator
				DeleteKeyFromNodeThread deleteKeyThread = new DeleteKeyFromNodeThread(coordinator, splitSelection[0]);
				deleteKeyThread.start(); deleteKeyThread.join();
				// delete from successor1
				deleteKeyThread = new DeleteKeyFromNodeThread(coordinator.succ1, splitSelection[0]);
				deleteKeyThread.start(); deleteKeyThread.join();
				// delete from successor2
				deleteKeyThread = new DeleteKeyFromNodeThread(coordinator.succ2, splitSelection[0]);
				deleteKeyThread.start(); deleteKeyThread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}
		return 0;
	}

	private int deleteLocalKey(String key) {
		if (fetchLocalRecordForKey(key) == null) {
			return 0;
		}

		MatrixCursor cursor = readAllLocalRecords();
		deleteAllLocalRecords();
		initializeRecordsFile();
		while (cursor.moveToNext()) {
			String existingKey = cursor.getString(cursor.getColumnIndex("key"));
			String existingValue = cursor.getString(cursor.getColumnIndex("value"));
			long timestamp = Long.parseLong(cursor.getString(cursor.getColumnIndex("timestamp")));
			if (!existingKey.equals(key)) {
				insertRecordLocally(existingKey, existingValue, timestamp);
			}
		}
		return 1;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		/* Protocol - key = key; value = value;timestamp;x
		if the request is to insert locally, then timestamp and x will not be null
		else the correct coordinator of the request will be found and the request will be forwarded

		// if x != null or timestamp != null, then blindly insert locally(stale request!)
		// 		if x == 1: then forward stale insertion request to the succ by reducing the x value
		// 		if x == 0: then do not forward
		// else
		// 		this is a fresh request!
		// 		find who is responsible and take actions accordingly
		 */

		final String key = (String)values.get("key");
		final String value = (String) values.get("value");
		String hashedKey = null;
		try {
			hashedKey = SimpleDynamoProvider.genHash(key);
		}catch (Exception e){
			Log.println(Log.DEBUG, "Exception:"+Global.MY_NODE_ID,"Hash method not found!");
			return null;
		}

		Log.println(Log.DEBUG, Global.myNode.avdName, "Log for key: "+key+", value: "+value);

		final String[] splitValue = value.split(";");
		if (splitValue.length == 1){
			// there are no timestamp;x components
			// find the responsible coordinator for the key
			Node keyCoordinator = GetCoordinatorForKey(key);
			Log.println(Log.DEBUG, "Log","Insert at "+Global.myNode.avdName+", coord: "+keyCoordinator.avdName);

			boolean insertLocally = false;
			ArrayList<String> nodes = new ArrayList<String>();
			if (keyCoordinator.avdName.equals(Global.myNode.avdName)){
				// insert at the start/coordinator
				Log.println(Log.DEBUG, Global.myNode.avdName, "Iam the coordinator");
				insertLocally = true;
				Log.println(Log.DEBUG, Global.myNode.avdName, "Succ1: "+keyCoordinator.succ1.avdName);
				nodes.add(keyCoordinator.succ1.avdName); // middle
				Log.println(Log.DEBUG, Global.myNode.avdName, "Succ1: "+keyCoordinator.succ2.avdName);
				nodes.add(keyCoordinator.succ2.avdName); // end
			}
			else{
				// none in the chain
				Log.println(Log.DEBUG, Global.myNode.avdName, "");
				nodes.add(keyCoordinator.avdName); // start
				nodes.add(keyCoordinator.succ1.avdName); //middle
				nodes.add(keyCoordinator.succ2.avdName); // end
			}

			/*else if (keyCoordinator.avdName.equals(Global.myNode.pred1.avdName)){
				// insert in the middle
				insertLocally = true;
				nodes.add(keyCoordinator.avdName); // start
				nodes.add(Global.myNode.succ1.avdName); // end
			}else if(keyCoordinator.avdName.equals(Global.myNode.pred2.avdName)){
				// insert in the end
				insertLocally = true;
				nodes.add(keyCoordinator.avdName); // start
				nodes.add(Global.myNode.pred1.avdName); // middle
			}*/

			final long currentTimestamp = System.currentTimeMillis();

			if(insertLocally){
				synchronized (SimpleDynamoProvider.class) {
					updateLocalRecord(key, value, currentTimestamp);
				}
			}

			RecordRow record = new RecordRow(key, value, currentTimestamp);
			ArrayList<Thread> threads = new ArrayList<Thread>();
			for (String node: nodes){
				InsertRecordIntoNode thread = new InsertRecordIntoNode(node, record);
				thread.start();
				threads.add(thread);
				Log.println(Log.DEBUG, "Log","Forwarding "+record.key+": "+record.value+" to: "+node);
			}
			// wait for the threads to finish
			for (Thread thread: threads){
				try {
					thread.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}else{
			// there is a timestamp;x component
			final long timestamp = Long.parseLong(splitValue[1]);
			int x = Integer.parseInt(splitValue[2]);
			// insert the key-value;timestamp locally
			synchronized (SimpleDynamoProvider.class) {
				updateLocalRecord(key, splitValue[0], timestamp);
			}
		}
		return null;
	}

	private static boolean isFirstGreater(String first, String second){
		int result = first.compareTo(second);
		return result > 0;
	}

	@Override
	public boolean onCreate() {
		boolean needToSync = recordsExist();

		if(!initializeRecordsFile()){
			Log.println(Log.DEBUG, "Init", "Could not create the RECORDS_FILE");
			return false;
		}
		Log.println(Log.DEBUG, "Init", "Initialization successful!");

		TelephonyManager tel = (TelephonyManager)  getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		Global.initialize(portStr);

		// communicate to the neighbours, and sync with them
		if (needToSync) {
			syncWithNeighbours();
		}

		// start a server on port 10000, and listen for requests(on a thread)
		try {
			ServerSocket serverSocket = new ServerSocket(Constants.SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
			//Toast.makeText(this, "Server socket created! ", Toast.LENGTH_SHORT).show();
		} catch (IOException e) {
			Toast.makeText(getContext().getApplicationContext(),
					"Can't create a server socket!", Toast.LENGTH_LONG).show();
		}

		return false;
	}

	private boolean recordsExist() {
		File file = new File(this.getContext().getFilesDir(), RECORDS_FILE);
		return file.exists();
	}

	private void syncWithNeighbours() {
		// sync with prececessors
		syncWithPredecessors();

		// sync with successors
		syncWithSuccessors();
	}

	private void syncWithSuccessors() {
		// ask the immediate successor for the key, values stored in my partition
		try {
			GetDataFromNodeForGivenPartition thread = new GetDataFromNodeForGivenPartition(
					Global.myNode.succ1.avdName,
					Global.myNode.avdName);
			thread.start();
			thread.join();

			HashMap<String, RecordRow> localRecords = null;

			// get local records
			synchronized (SimpleDynamoProvider.class) {
				// synchronizaton might not be required since syncing is done before the server starts
				localRecords = getLocalRecords();
			}
			if(localRecords == null){
				// there might be a case when nothing is present in this node but the successor is more updated.
				// add whatever data that it has
				localRecords = new HashMap<String, RecordRow>();
			}

			// sync up
			HashMap<String, RecordRow> successorRecords = thread.records;

			if(successorRecords == null){
				// successor does not have anything to add to this node..
				return;
			}
			for (String key : successorRecords.keySet()){
				RecordRow successorRecord = successorRecords.get(key);
				RecordRow localRecord = localRecords.get(key);
				if(localRecord == null){
					localRecords.put(key, successorRecord);
				}else{
					if (localRecord.timestamp < successorRecord.timestamp){
						localRecord.updateValue(successorRecord.value);
						localRecord.updateTimestamp(successorRecord.timestamp);
					}
				}
			}

			if(localRecords.size() != 0) {
				// write the database with the updated version of the localRecords
				synchronized (SimpleDynamoProvider.class) {
					updateLocalRecords(localRecords);
				}
			}

		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private void updateLocalRecords(HashMap<String, RecordRow> localRecords) {
		deleteAllLocalRecords();
		initializeRecordsFile();
		for (RecordRow recordRow: localRecords.values()){
			insertRecordLocally(recordRow.key, recordRow.value, recordRow.timestamp);
		}
	}

	private HashMap<String, RecordRow> getLocalRecords() {
		MatrixCursor localRecords = readAllLocalRecords();
		if (localRecords == null){
			return null;
		}
		HashMap<String, RecordRow> recordRowHashMap = new HashMap<String, RecordRow>();
		while(localRecords.moveToNext()){
			String key = localRecords.getString(localRecords.getColumnIndex("key"));
			String value = localRecords.getString(localRecords.getColumnIndex("value"));
			long timestamp = Long.parseLong(localRecords.getString(localRecords.getColumnIndex("timestamp")));
			RecordRow localRecord = new RecordRow(key, value, timestamp);
			recordRowHashMap.put(key, localRecord);
		}
		return recordRowHashMap;
	}

	private void syncWithPredecessors() {
		try {

			HashMap<String, RecordRow> localRecords = getLocalRecords();
			if (localRecords == null){
				localRecords = new HashMap<String, RecordRow>();
			}

			// get data in the partition of predecessor1 from predecessor1
			GetDataFromNodeForGivenPartition pred1Fetcher = new GetDataFromNodeForGivenPartition(
					Global.myNode.pred1.avdName,
					Global.myNode.pred1.avdName
			);
			pred1Fetcher.start();pred1Fetcher.join();

			// get data in the partition of predecessor2 from predecessor2
			GetDataFromNodeForGivenPartition pred2Fetcher = new GetDataFromNodeForGivenPartition(
					Global.myNode.pred2.avdName,
					Global.myNode.pred2.avdName
			);
			pred2Fetcher.start(); pred2Fetcher.join();

			HashMap<String, RecordRow> pred1Records = pred1Fetcher.records;
			HashMap<String, RecordRow> pred2Records = pred2Fetcher.records;

			if (pred1Records != null) {
				for (String pred1Key : pred1Records.keySet()) {
					RecordRow localRecord = localRecords.get(pred1Key);
					RecordRow pred1Record = pred1Records.get(pred1Key);
					if (localRecord == null) {
						localRecords.put(pred1Key, pred1Record);
					} else {
						if (localRecord.timestamp < pred1Record.timestamp) {
							localRecord.updateTimestamp(pred1Record.timestamp);
							localRecord.updateValue(pred1Record.value);
						}
					}
				}
			}

			if(pred2Records != null) {
				for (String pred2Key : pred2Records.keySet()) {
					RecordRow localRecord = localRecords.get(pred2Key);
					RecordRow pred2Record = pred2Records.get(pred2Key);
					if (localRecord == null) {
						localRecords.put(pred2Key, pred2Record);
					} else {
						if (localRecord.timestamp < pred2Record.timestamp) {
							localRecord.updateTimestamp(pred2Record.timestamp);
							localRecord.updateValue(pred2Record.value);
						}
					}
				}
			}
			updateLocalRecords(localRecords);
		}catch (Exception e){
			Log.println(Log.DEBUG, Global.MY_NODE_ID, "Error when syncing with the predecessors");
		}
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		if (selection.equals(SELECTION_ALL)){
			try {
				GetLocalRecordsFromAll localRecordCollector = new GetLocalRecordsFromAll(SELECTION_LOCAL);
				localRecordCollector.start();
				localRecordCollector.join(); // wait for it to finish
				return dropTimestamp(localRecordCollector.cursor);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}else if(selection.equals(SELECTION_LOCAL)){
			Log.println(Log.DEBUG, Global.MY_NODE_ID, "Logging data");
			//logLocalRecordContents();
			MatrixCursor records;
			synchronized (SimpleDynamoProvider.class) {
				 records = readAllLocalRecords();
			}
			MatrixCursor finalRecords = dropTimestamp(records);
			return finalRecords;
		}else if (selection.equals(SELECTION_LOCAL_WITH_TIMESTAMP)) {
			synchronized (SimpleDynamoProvider.class) {
				return readAllLocalRecords();
			}
		}else{
			/*
			Protocol - selection - key;Constants.ADDRESS_SELF
			We may not have Constants.ADDRESS_SELF when the query is passed by grader
			ADDRESS_SELF exists only when the request is to be forcefully handled by this node
			 */
			Log.println(Log.DEBUG, selection, "Query for key: "+selection);
			String[] splitKey = selection.split(";");
			String queryKey = splitKey[0];
			if (splitKey.length == 2 ){
				// this node has to take care of serving the request
				synchronized (SimpleDynamoProvider.class) {
					MatrixCursor cursor = fetchLocalRecordForKey(splitKey[0]);
					if (cursor == null){
						Log.println(Log.DEBUG, selection,"Key : "+splitKey[0]+" not found!");
						return null;
					}

					cursor.moveToNext();
					String value = cursor.getString(cursor.getColumnIndex("value"));
					String time = cursor.getString(cursor.getColumnIndex("timestamp"));
					Log.println(Log.DEBUG, selection,"Self result: val: "+value+", time: "+time);

					cursor = fetchLocalRecordForKey(splitKey[0]);
					return cursor;
				}
			}
			// querying for a specific key
			Node coordinator = GetCoordinatorForKey(selection);
			// the reader for the partition taken care of by the coordinator is the coordinator's
			try {
				GetKeyFromNode keyFetcherThread = new GetKeyFromNode(coordinator.succ2, selection);
				keyFetcherThread.start();

				GetKeyFromNode keyFetcherThread2 = new GetKeyFromNode(coordinator.succ1, selection);
				keyFetcherThread2.start();

				GetKeyFromNode keyFetcherThread3 = new GetKeyFromNode(coordinator, selection);
				keyFetcherThread3.start();

				keyFetcherThread.join(); keyFetcherThread2.join();keyFetcherThread3.join();

				String latestVal = null;
				long latestTimestamp = 0;
				if(keyFetcherThread.cursor == null){
					Log.println(Log.DEBUG, selection,"Fetch from "+coordinator.succ2.avdName+" failed");
				}else{
					Cursor cursor = keyFetcherThread.cursor;
					cursor.moveToFirst();
					String tempVal = cursor.getString(cursor.getColumnIndex("value"));
					long tempTime = Long.parseLong(cursor.getString(cursor.getColumnIndex("timestamp")));
					Log.println(Log.DEBUG, selection,"Result by node : "+coordinator.succ2.avdName+" val: "+tempVal+", ts: "+tempTime);
					if(latestTimestamp < tempTime){
						latestVal = tempVal;
						latestTimestamp = tempTime;
					}
				}

				if (keyFetcherThread2.cursor == null){
					Log.println(Log.DEBUG, selection,"Fetch from "+coordinator.succ1.avdName+" failed");
				}else{
					Cursor cursor = keyFetcherThread2.cursor;
					cursor.moveToFirst();
					String tempVal = cursor.getString(cursor.getColumnIndex("value"));
					long tempTime = Long.parseLong(cursor.getString(cursor.getColumnIndex("timestamp")));
					Log.println(Log.DEBUG, selection,"Result by node : "+coordinator.succ1.avdName+" val: "+tempVal+", ts: "+tempTime);
					if(latestTimestamp < tempTime){
						latestVal = tempVal;
						latestTimestamp = tempTime;
					}
				}

				if (keyFetcherThread3.cursor == null){
					Log.println(Log.DEBUG, selection,"Fetch from "+coordinator.avdName+" failed");
				}else{
					Cursor cursor = keyFetcherThread3.cursor;
					cursor.moveToFirst();
					String tempVal = cursor.getString(cursor.getColumnIndex("value"));
					long tempTime = Long.parseLong(cursor.getString(cursor.getColumnIndex("timestamp")));
					Log.println(Log.DEBUG, selection,"Result by node : "+coordinator.avdName+" val: "+tempVal+", ts: "+tempTime);
					if(latestTimestamp < tempTime){
						latestVal = tempVal;
						latestTimestamp = tempTime;
					}
				}

				Log.println(Log.DEBUG, selection,"Final res for key: "+selection+" val: "+latestVal+", ts: "+latestTimestamp);

				MatrixCursor finalCursor = new MatrixCursor(new String[]{"key", "value"});
				finalCursor.addRow(new String[]{selection, latestVal});
				return finalCursor;

				/*if (keyFetcherThread.cursor == null){
					Log.println(Log.DEBUG, Global.MY_NODE_ID, "Query to "+coordinator.succ2.avdName +
							" did not work return anything. key: "+selection);
					// talk to the immediate successor of the coordinator
					GetKeyFromNode keyFetcherThread2 = new GetKeyFromNode(coordinator.succ1, selection);
					keyFetcherThread2.start(); keyFetcherThread2.join();
					if(keyFetcherThread2.cursor == null){GetKeyFromNode keyFetcherThread3 = new GetKeyFromNode(coordinator, selection);
						keyFetcherThread3.start(); keyFetcherThread3.join();
						// talk to the coordinator
						Log.println(Log.DEBUG, Global.MY_NODE_ID, "Query to "+coordinator.succ1.avdName +
								" did not work return anything. key: "+selection);
						GetKeyFromNode keyFetcherThread3 = new GetKeyFromNode(coordinator, selection);
						keyFetcherThread3.start(); keyFetcherThread3.join();
						if(keyFetcherThread3.cursor == null){
							return null;
						}else{return dropTimestamp(keyFetcherThread3.cursor);}
					}else{
						Cursor res = keyFetcherThread2.cursor;
						/*res.moveToNext();
						String ke = res.getString(res.getColumnIndex("key"));
						String val = res.getString(res.getColumnIndex("value"));
						String ts = res.getString(res.getColumnIndex("timestamp"));
						res.moveToFirst();
						Log.println(Log.DEBUG, Global.myNode.avdHash,"Query result key: "+ke+", val: "+val+", ts: "+ts);
						return dropTimestamp(keyFetcherThread2.cursor);
					}
				}else
				{
					Cursor res = keyFetcherThread.cursor;
					/*res.moveToNext();
					String ke = res.getString(res.getColumnIndex("key"));
					String val = res.getString(res.getColumnIndex("value"));
					String ts = res.getString(res.getColumnIndex("timestamp"));
					Log.println(Log.DEBUG, Global.myNode.avdHash,"Query result key: "+ke+", val: "+val+", ts: "+ts);
					res.moveToFirst();
					return dropTimestamp(keyFetcherThread.cursor);
				}*/
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	private MatrixCursor dropTimestamp(MatrixCursor records) {
		if (records == null) {
			return null;
		}
		MatrixCursor simpleCursor = new MatrixCursor(new String[]{"key", "value"});
		while(records.moveToNext()){
			String key = records.getString(records.getColumnIndex("key"));
			String value = records.getString(records.getColumnIndex("value"));
			simpleCursor.addRow(new String[]{key, value});
		}
		return simpleCursor;
	}

	public static Node GetCoordinatorForKey(String key) {
		String hashedKey = null;
		try {
			hashedKey = SimpleDynamoProvider.genHash(key);
			Node keyCoordinator = null;
			for (int i = 0; i < 5; i++){
				Node node = Global.nodes.get(i);
				if(!isFirstGreater(hashedKey, node.avdHash)){ // hashedKey <= node.avdHash
					keyCoordinator = node;
					break;
				}
			}
			if(keyCoordinator == null){
				// this will happen when the hashedKey is greater than the largest node hash.
				// Node 0 is responsble for this key
				keyCoordinator = Global.nodes.get(0);
			}
			return keyCoordinator;
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			return null;
		}
	}

	private boolean initializeRecordsFile() {
		File file = new File(this.getContext().getFilesDir(), RECORDS_FILE);
		if (!file.exists()) {
			try {
				return file.createNewFile();
			}catch (Exception e){
				e.printStackTrace();
				Log.println(Log.DEBUG, "Init:"+Global.MY_NODE_ID, "Exception. Could not create the RECORDS_FILE");
				return false;
			}
		}
		return true;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    public static String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    // ------------------------------------------------Database utility functions -----------------------------------------//
	private boolean updateLocalRecord(String key, String newValue, long newTimestamp){
		Log.println(Log.DEBUG, Global.myNode.avdName, "Inserting "+key+", value "+newValue);
		MatrixCursor existingKeyRecord = fetchLocalRecordForKey(key);
		if (existingKeyRecord == null) {
			Log.println(Log.DEBUG, Global.myNode.avdName, key+" does not exist. Adding.");
			return insertRecordLocally(key, newValue, newTimestamp);
		} else {
			existingKeyRecord.moveToNext();
			String val = existingKeyRecord.getString(existingKeyRecord.getColumnIndex("value"));
			String tmp = existingKeyRecord.getString(existingKeyRecord.getColumnIndex("timestamp"));
			Log.println(Log.DEBUG, Global.myNode.avdName, "key: "+key+" found with value: "+val+", "+tmp+". New val: "+newValue+", "+newTimestamp);
			boolean updateDone = false;
			MatrixCursor allRecords = readAllLocalRecords();
			deleteAllLocalRecords();
			initializeRecordsFile();
			while (allRecords.moveToNext()) {
				String existingKey = allRecords.getString(allRecords.getColumnIndex("key"));
				String existingValue = allRecords.getString(allRecords.getColumnIndex("value"));
				long existingTimestamp = Long.parseLong(allRecords.getString(allRecords.getColumnIndex("timestamp")));
				if (existingKey.equals(key)) {
					if (existingTimestamp <= newTimestamp) {
						insertRecordLocally(existingKey, newValue, newTimestamp);
						updateDone = true;
					} else {
						insertRecordLocally(existingKey, existingValue, existingTimestamp);
					}
				} else {
					insertRecordLocally(existingKey, existingValue, existingTimestamp);
				}
			}
			return updateDone;
		}
	}

	private boolean insertRecordLocally(String key, String value, long timestamp) {
		Log.println(Log.DEBUG, Global.MY_NODE_ID, "Inserting locally: key:"+key+"value:"+value+"timestamp: "+timestamp);
		// open the records file
		File file = new File(this.getContext().getFilesDir(), RECORDS_FILE);
		FileWriter writer = null;
		try {
			writer = new FileWriter(file, true);
			String row = key+":"+value+":"+timestamp+"\n";
			Log.println(Log.DEBUG, Global.MY_NODE_ID, "Inserting :"+row);
			writer.append(row);
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
			Log.println(Log.DEBUG, "Exception:"+Global.MY_NODE_ID, "Could not append the file.");
			return false;
		}
		//logLocalRecordContents();
		return true;
	}

	private MatrixCursor fetchLocalRecordForKey(String key) {
		File file = new File(this.getContext().getFilesDir(), RECORDS_FILE);
		String value = null;
		String timestamp = null;
		try {
			FileReader reader = new FileReader(file);
			BufferedReader bufferedReader = new BufferedReader(reader);
			String line = null;
			while ((line = bufferedReader.readLine()) != null){
				String[] splitLine = line.split(":");
				if(splitLine[0].equals(key)){
					value = splitLine[1];
					timestamp = splitLine[2];
					break;
				}
			}
			bufferedReader.close();
			reader.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			Log.println(Log.ERROR, "Exception:"+Global.MY_NODE_ID, "Filenotfound");
		} catch (IOException e) {
			e.printStackTrace();
			Log.println(Log.ERROR, "Exception:"+Global.MY_NODE_ID, "IOExc");
		}

		if(value == null){
			return null;
		}else {
			MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value", "timestamp"});
			cursor.addRow(new String[]{key, value, timestamp});
			return cursor;
		}
	}

	private void logLocalRecordContents() {
		File file = new File(this.getContext().getFilesDir(), RECORDS_FILE);
		try {
			FileReader reader = new FileReader(file);
			BufferedReader bufferedReader = new BufferedReader(reader);
			String line = null;

			while ((line = bufferedReader.readLine()) != null){
				Log.println(Log.DEBUG, "RECORD_ROW", line);
			}
			bufferedReader.close();
			reader.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			Log.println(Log.ERROR, "Exception:"+Global.MY_NODE_ID,
					"FilenOtfound");
		} catch (IOException e) {
			e.printStackTrace();
			Log.println(Log.ERROR, "Exception:"+Global.MY_NODE_ID,
					"FilenOtfound");
		}
	}

	private MatrixCursor readAllLocalRecords() {
		MatrixCursor cursor = null;
		File file = new File(this.getContext().getFilesDir(), RECORDS_FILE);
		try {
			cursor = new MatrixCursor(new String[]{"key", "value", "timestamp"});
			FileReader reader = new FileReader(file);
			BufferedReader bufferedReader = new BufferedReader(reader);
			String line = null;
			int recordCount = 0;
			while ((line = bufferedReader.readLine()) != null) {
				// Log.println(Log.DEBUG, "RECORD_ROW", line);
				String[] splitLine = line.split(":");
				cursor.addRow(splitLine);
				recordCount++;
			}

			if (recordCount == 0) {
				cursor = null;
			}

			bufferedReader.close();
			reader.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			Log.println(Log.ERROR, "Exception:" + Global.MY_NODE_ID, "FileNotFound");
			return null;
		} catch (IOException e) {
			e.printStackTrace();
			Log.println(Log.ERROR, "Exception:" + Global.MY_NODE_ID, "IOExc");
			return null;
		}
		return cursor;

	}

	private void deleteAllLocalRecords() {
		File file = new File(this.getContext().getFilesDir(), RECORDS_FILE);
		if (file.exists()) {
			try {
				file.delete();
			}catch (Exception e){
				e.printStackTrace();
				Log.println(Log.DEBUG, "Init:"+Global.MY_NODE_ID, "Exception. Could not delete the RECORDS_FILE");;
			}
		}
	}

	// -------------------------------------------END -Database utility functions ------END--------------------------------//
	public class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			Uri mUri = Utils.buildUri();

			ServerSocket serverSocket = sockets[0];
			Socket connectionSocket = null;
			Looper.prepare();
			Handler handler = new Handler();
			try {
				while(true)
				{
					Log.println(Log.DEBUG, "Server", "Waiting for clients to connect");
					connectionSocket = serverSocket.accept();
					Log.println(Log.DEBUG, "Server", "Conneted to some client!");

					ContentResolver contentResolver = getContext().getApplicationContext().getContentResolver();

					// Spawn a new thread to handle the connection
					Runnable r = new ClientHandler(connectionSocket, contentResolver, mUri);
					//handler.post(r);
					Thread th = new Thread(r);
					th.start();
				}
			} catch (IOException e) {
				publishProgress(e.getMessage());
				e.printStackTrace();
				Log.println(Log.ERROR, "Server:"+ Global.myNode.avdName, e.getMessage());
			}
			return null;
		}

		protected void onProgressUpdate(String...strings) {
			Log.println(Log.DEBUG, "Server","Writing "+strings[0]+" to UI on server");
        /*String strReceived = strings[0].trim();
        TextView remoteTextView = (TextView) findViewById(R.id.textView1);
        remoteTextView.append(strReceived + "\n");*/
			return;
		}
	}

	public class GetLocalRecordsFromAll extends Thread{

		public MatrixCursor cursor;
		public String key;

		GetLocalRecordsFromAll(String key)
		{
			this.cursor = null;
			this.key = key;
		}

		public void run() {
			Socket nodeSocket = null;
			HashMap<String, RecordRow> keyToRecord = new HashMap<String, RecordRow>();
			for (Node node: Global.nodes){
				try {
					nodeSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							node.portNumber);
					DataInputStream inp = new DataInputStream(new BufferedInputStream(nodeSocket.getInputStream()));
					DataOutputStream out = new DataOutputStream(nodeSocket.getOutputStream());
					String message = Constants.MESSAGE_TYPE_QUERY + "-" + SELECTION_LOCAL_WITH_TIMESTAMP;
					out.writeUTF(message);

					// read all the key value pairs
					String records = inp.readUTF();
					if (records.equals(Constants.EMPTY_RESULT)) {
						cursor = null;
					} else {
						String[] recordsSplit = records.split(",");
						for (String record : recordsSplit) {
							String[] splitRecord = record.split(":");
							String[] splitValues = splitRecord[1].split(";");
							String key = splitRecord[0];
							String value = splitValues[0];
							long timestamp = Long.parseLong(splitValues[1]);
							RecordRow existingRow = keyToRecord.get(key);
							if (existingRow == null) {
								existingRow = new RecordRow(key, value, timestamp);
								keyToRecord.put(key, existingRow);
							} else {
								if (existingRow.timestamp <= timestamp) {
									existingRow.updateValue(value);
									existingRow.updateTimestamp(timestamp);
								}
							}
						}
					}
				}catch (Exception e){
					e.printStackTrace();
					Log.println(Log.ERROR, "Exception:"+Global.MY_NODE_ID, "IOExcept");
				}
			}

			this.cursor = new MatrixCursor(new String[]{"key", "value", "timestamp"});
			ArrayList<RecordRow> records = new ArrayList<RecordRow>(keyToRecord.values());
			for (RecordRow recordRow: records){
				this.cursor.addRow(recordRow.asStringArray());
			}
		}
	}

	public class GetKeyFromNode extends Thread{

		Node nodeToQuery;
		String key;
		MatrixCursor cursor;

		GetKeyFromNode(Node nodeToQuery, String key){
			this.nodeToQuery = nodeToQuery;
			this.key = key;
			this.cursor = null;
		}

		@Override
		public void run() {
			try{
				Socket socToReader1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						nodeToQuery.portNumber);

				DataInputStream inp = new DataInputStream(new BufferedInputStream(socToReader1.getInputStream()));
				DataOutputStream out = new DataOutputStream(socToReader1.getOutputStream());

				String request = Constants.MESSAGE_TYPE_QUERY+"-"+key+";"+Constants.ADDRESS_SELF;
				out.writeUTF(request);

				// wait for the response
				String response = inp.readUTF();
				// this will have only one line

				String[] splitResponse = response.split(":");
				String key = splitResponse[0];
				String valueTime = splitResponse[1];
				String[] valueSplit = valueTime.split(";");
				String value = valueSplit[0];
				String timestamp = valueSplit[1];
				this.cursor = new MatrixCursor(new String[]{"key", "value", "timestamp"});
				this.cursor.addRow(new String[]{key, value, timestamp});
			}catch (Exception e){
				this.cursor = null;
				Log.println(Log.DEBUG, Global.MY_NODE_ID, "Node "+this.nodeToQuery.avdName+", failed! "+e.getLocalizedMessage());
			}
		}
	}

	public class DeleteKeyFromNodeThread extends Thread{
		Node node;
		String key;

		DeleteKeyFromNodeThread(Node node, String key){
			this.node = node;
			this.key = key;
		}

		@Override
		public void run() {
			Socket nodeSocket = null;
			try{
				nodeSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						this.node.portNumber);

				DataInputStream inp = new DataInputStream(new BufferedInputStream(nodeSocket.getInputStream()));
				DataOutputStream out = new DataOutputStream(nodeSocket.getOutputStream());

				String command = Constants.MESSAGE_TYPE_DELETE+"-"+key+";"+Constants.ADDRESS_SELF;

				out.writeUTF(command);
			}catch(Exception exception){
				Log.println(Log.DEBUG, Global.MY_NODE_ID, "Exception when deleting from node.");
			}
		}
	}

	public class DeleteAllGlobally extends Thread{
		@Override
		public void run() {
			for (Node node: Global.nodes){
				try {
					Socket socketNode = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							node.portNumber);
					DataInputStream inp = new DataInputStream(new BufferedInputStream(socketNode.getInputStream()));
					DataOutputStream out = new DataOutputStream(socketNode.getOutputStream());
					String command = Constants.MESSAGE_TYPE_DELETE+"-"+SELECTION_LOCAL;
					out.writeUTF(command);
				} catch (IOException e) {
					e.printStackTrace();
				}

			}
		}
	}

	public class GetDataFromNodeForGivenPartition extends Thread{

		public HashMap<String, RecordRow> records = null;
		String nodeToGetDataFrom, nodeResponsibleForPartition;

		GetDataFromNodeForGivenPartition(String nodeToGetDataFrom, String nodeResponsibleForPartition){
			this.nodeToGetDataFrom = nodeToGetDataFrom;
			this.nodeResponsibleForPartition = nodeResponsibleForPartition;
		}

		@Override
		public void run() {
			String request = Constants.MESSAGE_TYPE_SYNC_REQUEST +"-"+this.nodeResponsibleForPartition+";"+Global.MY_NODE_ID;
			Socket socketNode = null;
			int nodeToGetDataFromPort = Utils.avdNameToPort(this.nodeToGetDataFrom);
			try {
				socketNode = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						nodeToGetDataFromPort);

				DataInputStream inp = new DataInputStream(new BufferedInputStream(socketNode.getInputStream()));
				DataOutputStream out = new DataOutputStream(socketNode.getOutputStream());

				out.writeUTF(request);

				String response = inp.readUTF();
				if(response.equals(Constants.EMPTY_RESULT)){
					return;
				}

				this.records = new HashMap<String, RecordRow>();

				String[] records = response.split(",");
				for(String recordLine: records){
					String keyValue[] = recordLine.split(":");
					String key = keyValue[0];
					String[] valueTimestamp = keyValue[1].split(";");
					String value = valueTimestamp[0];
					long timestamp = Long.parseLong(valueTimestamp[1]);
					RecordRow record = new RecordRow(key, value, timestamp);
					this.records.put(key, record);
				}
				socketNode.close();
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}


	public class InsertRecordIntoNode extends Thread{

		String nodeName;
		RecordRow recordRow;

		InsertRecordIntoNode(String nodeName, RecordRow recordRow){
			this.nodeName = nodeName;
			this.recordRow = recordRow;
		}

		@Override
		public void run() {
			try{
				String insertRequest = Constants.MESSAGE_TYPE_INSERT+"-"+recordRow.key+":"+recordRow.value+";"+recordRow.timestamp+";0";
				int port = Utils.avdNameToPort(this.nodeName);
				Socket socToSucc = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						port);
				DataOutputStream out = new DataOutputStream(socToSucc.getOutputStream());
				out.writeUTF(insertRequest);
			}catch (Exception e){

			}
		}
	}

}
