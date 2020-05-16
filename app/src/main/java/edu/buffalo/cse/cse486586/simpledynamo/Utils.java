package edu.buffalo.cse.cse486586.simpledynamo;

import android.database.Cursor;
import android.net.Uri;

import java.util.ArrayList;

public class Utils {
    public static Uri buildUri() {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(Constants.CP_AUTHORITY);
        uriBuilder.scheme(Constants.CP_SCHEME);
        return uriBuilder.build();
    }

    public static String convertCursorToString(Cursor cursor) {
        String result = "";
        ArrayList<String> keyValuePairs = new ArrayList<String>();
        StringBuilder sb = new StringBuilder();
        while(cursor.moveToNext()){
            int keyIndex = cursor.getColumnIndex("key");
            int valueIndex = cursor.getColumnIndex("value");
            String returnKey = cursor.getString(keyIndex);
            String returnValue = cursor.getString(valueIndex);
            String timestamp = cursor.getString(cursor.getColumnIndex("timestamp"));
            sb.append(returnKey+":"+returnValue+";"+timestamp).append(",");
        }
        if (sb.length() != 0)
        {
            result = sb.deleteCharAt(sb.length() - 1).toString();
        }
        if (result.equals("")){
            return Constants.EMPTY_RESULT;
        }
        return  result;
    }

    public static int avdNameToPort(String nodeName) {
        return Integer.parseInt(nodeName) * 2;
    }

    public static String filterDataInPartition(Cursor result, String otherAvd) {
        StringBuilder sb = new StringBuilder();
        while(result.moveToNext()){
            String key = result.getString(result.getColumnIndex("key"));
            String value = result.getString(result.getColumnIndex("value"));
            String timestamp = result.getString(result.getColumnIndex("timestamp"));
            Node coordinator = SimpleDynamoProvider.GetCoordinatorForKey(key);
            if(!coordinator.avdName.equals(otherAvd)){
                continue;
            }
            sb.append(key+":"+value+";"+timestamp).append(",");
        }
        if(sb.length() == 0){
            return Constants.EMPTY_RESULT;
        }
        String strResult = sb.deleteCharAt(sb.length() - 1).toString();
        return strResult;
    }
}
