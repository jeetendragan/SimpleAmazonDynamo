package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.os.Bundle;
import android.app.Activity;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;
import android.widget.Toast;

public class SimpleDynamoActivity extends Activity {

	EditText etKey;
	Button btnInsert, btnQuery, btnDelete;

	ContentResolver cr;
	Uri uri;

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);
    
		final TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

		etKey = (EditText) findViewById(R.id.etKey);
		btnInsert = (Button) findViewById(R.id.btnInsert);
		btnQuery = (Button) findViewById(R.id.btnQuery);
		btnDelete = (Button) findViewById(R.id.btnDelete);
		this.cr = getContentResolver();
		this.uri = Utils.buildUri();

		btnInsert.setOnClickListener(new View.OnClickListener() {
			@Override
			public void onClick(View v) {
				String insertKeyValue = (String) etKey.getText().toString();
				String[] splitInsert = insertKeyValue.split(":");
				String key = splitInsert[0];
				String value = splitInsert[1];
				ContentValues contentValues = new ContentValues();
				contentValues.put("key", key);
				contentValues.put("value", value);
				cr.insert(uri, contentValues);
				tv.setText("Insert done");
			}
		});

		btnQuery.setOnClickListener(new View.OnClickListener() {
			@Override
			public void onClick(View v) {
				String key = etKey.getText().toString();
				Cursor result = cr.query(uri, null, key, null, null);
				String str = "";
				if (result == null){
					str = Constants.EMPTY_RESULT;
				}else {
					while (result.moveToNext()) {
						int keyIndex = result.getColumnIndex("key");
						int valueIndex = result.getColumnIndex("value");
						String returnKey = result.getString(keyIndex);
						String returnValue = result.getString(valueIndex);
						str += returnKey + ":" + returnValue+"\n";
					}
				}
				tv.setText(str);
			}
		});

		btnDelete.setOnClickListener(new View.OnClickListener() {
			@Override
			public void onClick(View v) {
				try {
					ContentResolver mContentResolver = getContentResolver();
					Uri mUri = Utils.buildUri();
					String key = etKey.getText().toString();
					mContentResolver.delete(mUri, key, null);
					tv.setText("Delete done");
				}catch (Exception exception){
					Toast.makeText(getApplicationContext(), "Delete exception.", Toast.LENGTH_LONG).show();
				}
			}
		});
	}

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.simple_dynamo, menu);
		return true;
	}
	
	public void onStop() {
        super.onStop();
	    Log.v("Test", "onStop()");
	}

}
