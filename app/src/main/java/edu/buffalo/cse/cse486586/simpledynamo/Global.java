package edu.buffalo.cse.cse486586.simpledynamo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class Global {
    public static ArrayList<Node> nodes;
    public static Node myNode;
    public static String MY_NODE_ID;

    public static void initialize(String myNodeName) {
        nodes = new ArrayList<Node>();
        nodes.add(new Node(Constants.AVD_0));
        nodes.add(new Node(Constants.AVD_1));
        nodes.add(new Node(Constants.AVD_2));
        nodes.add(new Node(Constants.AVD_3));
        nodes.add(new Node(Constants.AVD_4));

        Collections.sort(nodes, new Comparator<Node>() {
            @Override
            public int compare(Node lhs, Node rhs) {
                return lhs.avdHash.compareTo(rhs.avdHash);
            }
        });

        for (int i = 0; i < nodes.size(); i++){
            int succ1 = (i + 1) % 5;
            int succ2 = (i + 2) % 5;
            Node node = nodes.get(i);
            node.succ1 = nodes.get(succ1);
            node.succ2 = nodes.get(succ2);

            if (node.avdName.equals(myNodeName)) {
                myNode = node;
                MY_NODE_ID = myNode.avdHash;
            }

        }

        // set predecessors
        Node node = nodes.get(0);
        node.pred1 = nodes.get(4);
        node.pred2 = nodes.get(3);

        node = nodes.get(1);
        node.pred1 = nodes.get(0);
        node.pred2 = nodes.get(4);

        node = nodes.get(2);
        node.pred1 = nodes.get(1);
        node.pred2 = nodes.get(0);

        node = nodes.get(3);
        node.pred1 = nodes.get(2);
        node.pred2 = nodes.get(1);

        node = nodes.get(4);
        node.pred1 = nodes.get(3);
        node.pred2 = nodes.get(2);

    }
}
