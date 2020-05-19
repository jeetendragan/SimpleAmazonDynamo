package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.util.Log;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class ClientHandler implements  Runnable{

    Socket connectionSocket;
    ContentResolver contentResolver;
    Uri mUri;

    public ClientHandler(Socket connectionSocket, ContentResolver contentResolver, Uri mUri){
        this.connectionSocket = connectionSocket;
        this.contentResolver = contentResolver;
        this.mUri = mUri;
    }

    @Override
    public void run() {
        try{
            DataInputStream inp = new DataInputStream(new BufferedInputStream(connectionSocket.getInputStream()));
            DataOutputStream out = new DataOutputStream(connectionSocket.getOutputStream());

            // read the request type
            while(inp.available() < 0){;}
            String request = inp.readUTF();
            Log.println(Log.DEBUG, Global.MY_NODE_ID+" server", "Request:"+request);

            String[] splitRequest = request.split("-");
            String requestType = splitRequest[0];

            if(requestType.equals(Constants.MESSAGE_TYPE_INSERT)){
                String insertionData = splitRequest[1];
                String[] keyValue = insertionData.split(":");
                String key = keyValue[0];
                String value = keyValue[1];

                Log.println(Log.DEBUG, "Server at: "+Global.MY_NODE_ID,
                        "Insert request for key:"+key);

                ContentValues cv = new ContentValues();
                cv.put("key", key);
                cv.put("value", value);

                // call the local content provider to check if data can be inserted on this AVD
                // else it will be forwarded
                contentResolver.insert(mUri, cv);

                out.writeUTF(Constants.ACK);

                connectionSocket.close();
            }
            if(requestType.equals(Constants.MESSAGE_TYPE_QUERY)){
                String key = splitRequest[1];
                if(!key.equals(SimpleDynamoProvider.SELECTION_ALL)){
                    // query the local content provider
                    Cursor result = contentResolver.query(mUri, null,
                            key, null, null);
                    if(result != null) {
                        String stringResult = Utils.convertCursorToString(result);
                        out.writeUTF(stringResult);
                    }else {
                        out.writeUTF(Constants.EMPTY_RESULT);
                    }
                }
            }
            if(requestType.equals(Constants.MESSAGE_TYPE_DELETE)){
                String key = splitRequest[1];
                contentResolver.delete(mUri, key, null);
                connectionSocket.close();
                /*if(key.equals(SimpleDhtProvider.SELECTION_ALL)){
                    // * will be passed on as a query parameter only to the node_0(5554)
                    String nodesAlive = "";
                    for (Node node : SimpleDhtProvider.serverNodes){
                        nodesAlive += node.avdName+":";
                    }
                    nodesAlive = nodesAlive.substring(0, nodesAlive.length() - 1);
                    out.writeUTF(nodesAlive);
                }else if(key.equals(SimpleDhtProvider.SELECTION_LOCAL)){
                    // delete everything at this local node
                    contentResolver.delete(mUri, SimpleDhtProvider.SELECTION_LOCAL, null);
                    connectionSocket.close();
                }else{
                    // delete a specific key
                    contentResolver.delete(mUri, key, null);
                    connectionSocket.close();
                }*/
            }
            if(requestType.equals(Constants.MESSAGE_TYPE_SYNC_REQUEST)){
                String[] avdsToGetDataFrom = splitRequest[1].split(";");
                Cursor result = contentResolver.query(mUri, null,
                        SimpleDynamoProvider.SELECTION_LOCAL_WITH_TIMESTAMP,
                        null, null);
                if (result == null){
                    out.writeUTF(Constants.EMPTY_RESULT);
                }else {
                    String filteredData = Utils.filterDataInPartition(result, avdsToGetDataFrom);
                    String logResult = Utils.filterDataInPartitionKeysOnly(result, avdsToGetDataFrom);
                    Log.println(Log.DEBUG, Constants.MESSAGE_TYPE_SYNC_REQUEST, " Keys: "+logResult);
                    out.writeUTF(filteredData);
                }
                Log.println(Log.DEBUG, "Server:"+Global.MY_NODE_ID, "Data for sync request sent back");
            }
        } catch (IOException e) {
            //publishProgress(e.getMessage());
            e.printStackTrace();
            Log.println(Log.ERROR, "Server:"+Global.MY_NODE_ID,
                    "IOEXCEPTIon occured bro!");
        }
     }
}
