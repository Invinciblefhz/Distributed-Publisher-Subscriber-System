package brokerSystem;

import java.io.*;
import java.net.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * The PublisherHandler class handles the communication between a Publisher and the Broker in a publish-subscribe system.
 * It allows Publishers to create topics, delete topics, publish messages, and handle other related commands.
 * This class implements the Runnable interface to enable concurrent handling of Publisher requests.
 * 
 * @author Hanzhou Fang
 * student id:1166053
 * 
 */
public class PublisherHandler implements Runnable {
    private Socket socket;
    private Broker broker;
    private BufferedReader in;
    private PrintWriter out;
    private String name;

    /**
     * Constructs a PublisherHandler to manage the connection and communication between a Publisher and the Broker.
     * 
     * @param socket the Socket representing the connection to the Publisher
     * @param broker the Broker managing the publish-subscribe system
     */
    public PublisherHandler(Socket socket, Broker broker) {
        this.socket = socket;
        this.broker = broker;
        try {
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            out = new PrintWriter(socket.getOutputStream(), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Listens for messages from the Publisher and processes commands such as creating, deleting, and publishing topics.
     * This method is executed in a separate thread to handle requests concurrently.
     */
    @Override
    public void run() {
        String message;
        try {
            while ((message = in.readLine()) != null) {
            	System.out.println("Recieve and handle the message from publisher " + name + ":" + message);
                String[] parts = message.split(",", 3);
                String command = parts[0];

                if (command.equals("create") && parts.length == 3) {
                    String topicID = parts[1];
                    String topicName = parts[2];
                    String authorName = in.readLine();
                    this.name = authorName;
                    if (!broker.checkTopicExist(topicID)) {
                    	broker.handleCreateTopic(topicID, topicName, authorName);
                        message = message + "," + authorName;
                        out.println("success");
                        broker.broadcastToOtherBrokers(message);
                    } else {
                    	out.println("error: The topicID is already exists");
                    }
                    
                }
                
                else if (command.equals("delete") && parts.length == 2) {
                	String topicID = parts[1];
                	if (broker.handleDelete(topicID,name,out)) {
                		out.println("success");
                		broker.broadcastToOtherBrokers(message);
                	}
                	
                }
                else if (command.equals("show") && parts.length == 2) {
                	String topicID = parts[1];
                	broker.handleShow(topicID, out,this);
                }
                else if (command.equals("publish") && parts.length == 3) {
                	String topicID = parts[1];
                	String content = parts[2];
                	DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM HH:mm:ss");
                    String currentTime = LocalDateTime.now().format(formatter);

                    String formattedMessage = String.format("%s %s:%s: %s", currentTime, topicID, broker.getTopic(topicID).getName(), content);
                    String transitMessage = "publish," + topicID + "," + formattedMessage;
                    if (broker.checkTopicExist(topicID)) {
                    	if (broker.getTopic(topicID).getAuthorName().equals(name)) {
                    		broker.handlePublic(topicID, formattedMessage);
                        	broker.broadcastToOtherBrokers(transitMessage);
                        	out.println("success");
                    	}
                    	else {
                    		out.println("error: This topic is not belonging to you.");
                    	}
                    	
                    }
                    else {
                    	out.println("error: The topic you enter is not exist.");
                    }
                }
                else if (command.equals("disconnect") && parts.length == 1) {
                   
                    broker.handlePublisherDisconnect(name);
                }
                out.println("Please select command: create, publish, show, delete.");
                
            }
        } catch (IOException e) {
        	System.out.println("Publisher " + name + " disconnected");
        	broker.handlePublisherDisconnect(this);
        }
    }
    
    /**
     * Sets the name of the Publisher associated with this handler.
     * 
     * @param name the name of the Publisher
     */
    public void setName(String name) {
    	this.name = name;
    }
    
    /**
     * Returns the name of the Publisher associated with this handler.
     * 
     * @return the Publisher's name
     */
    public String getName() {
    	return name;
    }
}
