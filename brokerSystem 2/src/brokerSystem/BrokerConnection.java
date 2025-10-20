package brokerSystem;

/**
 * @author Hanzhou Fang
 * student id:1166053
 */

import java.io.*;
import java.net.*;

/**
 * The BrokerConnection class handles the connection between Brokers in a distributed publish-subscribe system.
 * It allows Brokers to communicate and exchange messages such as creating, deleting, subscribing, and publishing topics.
 * This class implements the Runnable interface to allow concurrent handling of messages from other Brokers.
 */
public class BrokerConnection implements Runnable {
    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;
    private Broker broker;

    /**
     * Constructs a BrokerConnection that handles communication between the current Broker and another Broker.
     * 
     * @param socket the Socket representing the connection to another Broker
     * @param broker the Broker object managing this connection
     */
    public BrokerConnection(Socket socket, Broker broker) {
        this.socket = socket;
        this.broker = broker;
        try {
            out = new PrintWriter(socket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
    * Listens for incoming messages from other Brokers and processes them.
    * This method is executed in a separate thread for concurrent message handling.
    */

    @Override
    public void run() {
        String message;
        try {
            while ((message = in.readLine()) != null) {
                System.out.println("Received and handling message from another broker:" + message);
                handleBrokerMessage(message);  
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    /**
     * Handles incoming messages from other Brokers by parsing the command and taking the appropriate action,
     * such as creating or deleting topics, subscribing or unsubscribing to topics, or publishing messages.
     * 
     * @param message the message received from another Broker
     */
    public void handleBrokerMessage(String message) {
        String[] parts = message.split(",", 3);
        String command = parts[0];

        if (command.equals("create") && parts.length == 3) {
            String topicID = parts[1];
            String[] newString = parts[2].split(",",2);
            String topicName = newString[0];
            String authorName = newString[1];
            broker.handleCreateTopic(topicID, topicName,authorName); 
        } else if (command.equals("delete") && parts.length == 2) {
            String topicID = parts[1];
            broker.handleDelete(topicID);
        } else if (command.equals("add") && parts.length == 3) {
         	String topicID = parts[1];
         	String name = parts[2];
        	broker.subTopic(topicID,name);
        } else if (command.equals("unsub")) {
        	    String topicID = parts[1];
           	String name = parts[2];
        	broker.unsubTopic(topicID,name);
        } else if (command.equals("publish")) {
          	String topicID = parts[1];
          	String content = parts[2];
        	broker.handlePublic(topicID, content);
        }
        
    }

    /**
     * Sends a message to the connected Broker.
     * 
     * @param message the message to send to the other Broker
     */
    public void sendMessage(String message) {
        out.println(message);
    }
}
