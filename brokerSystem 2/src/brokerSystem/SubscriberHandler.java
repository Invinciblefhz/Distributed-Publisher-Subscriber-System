package brokerSystem;


import java.io.*;
import java.net.*;
import java.util.List;
import java.util.ArrayList;


/**
 * The SubscriberHandler class manages the communication between a Subscriber and the Broker in a 
 * publish-subscribe system. It handles various Subscriber commands, such as listing topics, subscribing 
 * to topics, unsubscribing, and receiving published messages. This class implements the Runnable interface 
 * to allow concurrent handling of multiple subscribers.
 * 
 * @author Hanzhou Fang
 * student id:1166053
 */
public class SubscriberHandler implements Runnable {
    private Socket socket;
    private Broker broker;
    private BufferedReader in;
    private PrintWriter out;
    private List<String> subscribeTopic;
    private String name;
    

    /**
     * Constructs a SubscriberHandler to manage the communication between the Subscriber and the Broker.
     * 
     * @param socket the Socket representing the connection to the Subscriber
     * @param broker the Broker managing the publish-subscribe system
     */
    public SubscriberHandler(Socket socket, Broker broker) {
        this.socket = socket;
        this.broker = broker;
        subscribeTopic = new ArrayList<>();
        try {
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            out = new PrintWriter(socket.getOutputStream(), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    /**
     * Sends a published message to the subscriber if the subscriber is subscribed to the given topic.
     * 
     * @param topicID the ID of the topic the message is being published to
     * @param content the content of the published message
     */
    public void publicMessage(String topicID, String content) {

    	if (subscribeTopic.contains(topicID)) {
    		out.println(content);
    		out.println("Please select command: list, sub, current, unsub.");
    	}
    }

    /**
     * Removes a topic from the subscriber's list of subscribed topics and notifies the subscriber 
     * that the topic has been deleted by the publisher.
     * 
     * @param topicID the ID of the topic to be removed
     */
    public void removeTopic(String topicID) {
    	if (subscribeTopic.contains(topicID)) {
    		subscribeTopic.remove(topicID);
    		out.println(topicID + " is deleted by the publisher");
    		out.println("Please select command: list, sub, current, unsub.");
    	}
    }
    
    /**
     * Listens for messages from the Subscriber and processes commands such as listing topics, subscribing, 
     * unsubscribing, and disconnecting. Runs in a separate thread for concurrent execution.
     */
    @Override
    public void run() {
        try {
            String message;
            while ((message = in.readLine()) != null) {
            	System.out.println("Receive and handle message from subscriber: " + message);
                String[] parts = message.split(",");
                String command = parts[0];
          
                
                if (command.equals("list") && parts.length == 2 && parts[1].equals("all")) {
                    broker.listAllTopic(out);
                } else if (command.equals("sub") && parts.length == 2) {
                    String topicID = parts[1];
                    if (!subscribeTopic.contains(topicID)) {
                    	if (broker.subTopic(topicID,name)) {
                        	broker.broadcastToOtherBrokers("add," + topicID + "," + name);
                        	subscribeTopic.add(topicID);
                        	out.println("success");
                       }
                    	else {
                    		out.println("error: The topic does not exists");
                    	}
                    
                    }
                    else {
                    	out.println("error: You already subscibe this topic.");
                    }
                } else if (command.equals("current")) {
                	if (subscribeTopic.isEmpty()) {
                		out.println("Currently you have't subscribed any topic.");
                	}
                	else {
                		broker.listTopic(subscribeTopic, out);
                	}
                	
                } else if (command.equals("unsub")) {
                	String topicID = parts[1];
                	if (subscribeTopic.contains(topicID)) {
                		broker.unsubTopic(topicID,name);
                		subscribeTopic.remove(topicID);
                		broker.broadcastToOtherBrokers("unsub,"+topicID + "," + name);
                		out.println("success");
                	}
                	else {
                		out.println("error: You haven't subscribe this topic");
                	}
                } else if (command.equals("disconnect") && parts.length == 1) {
                    broker.handleSubscriberDisconnect(this,name);
                }
                
                out.println("Please select command: list, sub, current, unsub.");
            }
            
        } catch (IOException e) {
            System.out.println("Subscriber " + name + "disconnected.");
            broker.handleSubscriberDisconnect(this, name);
        }
        
        
    }
    public void setName(String name) {
    	this.name = name;
    }
    
    public List<String> getSubscriberTopic() {
    	return subscribeTopic;
    }
}
