package edu.buffalo.cse.cse486586.simpledynamo;

import android.util.Log;

import java.security.NoSuchAlgorithmException;

public class Node {
    String avdName;
    String avdHash;
    int portNumber;
    Node succ1;
    Node succ2;
    Node pred1;
    Node pred2;

    public Node(String avdName){
        this.avdName = avdName;
        this.portNumber = Integer.parseInt(avdName) * 2;
        try {
            this.avdHash = SimpleDynamoProvider.genHash(avdName);
        }catch (NoSuchAlgorithmException exception){
            Log.println(Log.DEBUG, "Node", exception.getMessage());
        }
        this.succ1 = null;
        this.succ2 = null;
        this.pred1 = null;
        this.pred2 =null;
    }

    @Override
    public String toString() {
        return "AvdName:"+avdName+", Hash:"+avdHash+", Port:"+portNumber+"Succ_1: "+this.succ1.avdName+", Succ_2: "+this.succ2.avdName+" Pred1:"+pred1+", Pred2:"+pred2;
    }
}
