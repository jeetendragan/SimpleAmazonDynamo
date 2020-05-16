package edu.buffalo.cse.cse486586.simpledynamo;

public class RecordRow {
    String key, value;
    long timestamp;

    RecordRow(String key, String value, long timestamp){
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }

    public void updateValue(String value) {
        this.value = value;
    }

    public void updateTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String[] asStringArray() {
        return new String[]{this.key, this.value, this.timestamp+""};
    }
}
