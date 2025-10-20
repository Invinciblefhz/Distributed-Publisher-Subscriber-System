package brokerSystem;


import java.io.*;
import java.net.*;
import java.util.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * The Broker class is responsible for managing connections between Publishers, Subscribers,
 * and other Brokers. It allows topic creation, message broadcasting, and registration with
 * a Directory Service for broker discovery.
 * 
 * @author Hanzhou Fang
 * student id:1166053
 */
public class Broker {
    private ServerSocket serverSocket;
    private List<BrokerConnection> brokerConnections; 
    private int port;
    private Map<String, Topic> topicMap;
    private Map<Socket, String> publisherNames;
    private List<SubscriberHandler> subscriberConnections;

    
    /**
     * Constructs a Broker with the specified port.
     * 
     * @param port the port number on which the Broker listens for connections
     */
    public Broker(int port) {
        this.port = port;
        brokerConnections = new ArrayList<>();
        topicMap = new HashMap<>();
        publisherNames = new HashMap<>();
        subscriberConnections = new ArrayList<>();
    }

    /**
     * Starts the Broker, registers it with the Directory Service, and accepts connections
     * from Publishers, Subscribers, and other Brokers.
     * 
     * @param directoryServiceIP   the IP address of the Directory Service
     * @param directoryServicePort the port of the Directory Service
     */
    public void startBroker(String directoryServiceIP, int directoryServicePort) {
        try {
        	
          	List<String> brokerList = registerWithDirectoryService(directoryServiceIP, directoryServicePort);
            connectToExistingBrokers(brokerList);
            serverSocket = new ServerSocket(port);
            System.out.println("Broker started on port " + port);

            
            new Thread(() -> {
                while (true) {
                    try {
                        Socket socket = serverSocket.accept();
                        ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
                        String clientType = input.readUTF();  // read the connection type.

                        if (clientType.equals("PUBLISHER")) {
                            	String publisherName = input.readUTF();  // read the publisher name
                            publisherNames.put(socket, publisherName);
                            PublisherHandler publisher = new PublisherHandler(socket, this);
                            publisher.setName(publisherName);
                            System.out.println("Publisher connected: " + publisherName);
                            new Thread(publisher).start(); // thread to deal with publisher command
                        } else if (clientType.equals("BROKER")) {
                            // deal with the connection between the broker
                            BrokerConnection connection = new BrokerConnection(socket, this);
                            brokerConnections.add(connection);
                            System.out.println("New broker connected");
                            new Thread(connection).start();
                        } else if (clientType.equals("SUBSCRIBER")) {
                         	// deal with the connection between the subscriber
                        	    String subscriberName = input.readUTF();
                           	SubscriberHandler subscriber = new SubscriberHandler(socket, this);
                          	subscriber.setName(subscriberName);
                         	subscriberConnections.add(subscriber);
                         	System.out.println("Subsciber " + subscriberName + " connected.");
                            new Thread(subscriber).start();                           
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    /**
     * Checks if a topic with the given ID exists.
     * 
     * @param topicID the ID of the topic to check
     * @return true if the topic exists, false otherwise
     */
    public boolean checkTopicExist(String topicID) {
    	    return topicMap.containsKey(topicID);
    }
    

    /**
     * Connects to an existing Broker.
     * 
     * @param brokerIP   the IP address of the Broker
     * @param brokerPort the port number of the Broker
     */
    public void connectToBroker(String brokerIP, int brokerPort) {
        try {
            Socket socket = new Socket(brokerIP, brokerPort);
            
            ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
            output.writeUTF("BROKER");
            output.flush();

            BrokerConnection connection = new BrokerConnection(socket, this);
            brokerConnections.add(connection);
            new Thread(connection).start();
            System.out.println("Connected to Broker at " + brokerIP + ":" + brokerPort);
        } catch (IOException e) {
            System.err.println("Failed to connect to Broker at " + brokerIP + ":" + brokerPort);
        }
    }
    
    /**
     * Broadcasts a message to all connected Brokers.
     * 
     * @param message the message to broadcast
     */
    public void broadcastToOtherBrokers(String message) {
        System.out.println("Broadcasting message: " + message);
        for (BrokerConnection connection : brokerConnections) {
            connection.sendMessage(message);
        }
    }
    
    

    /**
     * Handles the 'show' command for topics, showing the details of a specific topic or all topics created by a publisher.
     * 
     * @param topicID   the ID of the topic to show, or 'all' to show all topics by the publisher
     * @param out       the PrintWriter for sending responses to the client
     * @param publisher the PublisherHandler responsible for the request
     */
    public void handleShow(String topicID, PrintWriter out, PublisherHandler publisher) {
    	    if (topicID.equals("all")) {
    	    	int count = 0;
    		    for (Topic topic : topicMap.values()) {
        		    if (publisher.getName().equals(topic.getAuthorName())) {
        			    out.println(topic.showTopic());
        			    count++;
        		    }
        	    }
    	    	if (count == 0) {
    	    		out.println("You haven't create any topic.");
    	    	}
        	}
    	    else {
    		    if (topicMap.containsKey(topicID)) {
    			    if (publisher.getName().equals(topicMap.get(topicID).getAuthorName())) {
    				    out.println(topicMap.get(topicID).showTopic());
    			    }
    			    else {
    				    out.println("It is not your topic");
    			    }
    		    } else {
    			    out.println("The topic don't exist");
    		    }
      	}
    	
    }
    
    /**
     * Publishes a message to all subscribers of a given topic.
     * 
     * @param topicID the ID of the topic to which the message is published
     * @param content the content of the message
     */
    public void handlePublic(String topicID, String content) {
    	
     	for (SubscriberHandler connection : subscriberConnections) {
    		    connection.publicMessage(topicID,  content);
        }
    }
    
    /**
     * Lists all topics for the connected Subscriber.
     * 
     * @param out the PrintWriter for sending the list to the client
     */
    public void listAllTopic(PrintWriter out) {
    	if (topicMap.isEmpty()) {
    		out.println("There is no topic right now.");
    	}
     	for (Topic topic : topicMap.values()) {
    		    out.println(topic.listTopic());
        } 
    }
    
    /**
     * Subscribes a client to a topic by topic ID.
     * 
     * @param printID the ID of the topic
     * @param name    the name of the subscriber
     * @return true if subscription was successful, false otherwise
     */
    public boolean subTopic(String printID, String name) {
    	    if (topicMap.containsKey(printID)) {
    		    Topic topic = topicMap.get(printID);
    		    topic.addSub(name);
    		    return true;
     	    } 
    	    else {
    		    return false;
     	}
    }
    
    
    public void listTopic(List<String> topicIDs, PrintWriter out) {
    	    for (String topicID : topicIDs) {
    		    if (topicMap.containsKey(topicID)) {
    			    out.println(topicMap.get(topicID).listTopic());
    		    }
    		
      	}
    }
    
    /**
     * Unsubscribes a client from a topic by topic ID.
     * 
     * @param printID the ID of the topic
     * @param name    the name of the subscriber
     */
    public void unsubTopic(String printID, String name) {
    	    topicMap.get(printID).unsub(name);
    }
    
    /**
     * Creates a new topic with the given ID, name, and author.
     * 
     * @param topicID   the ID of the topic
     * @param topicName the name of the topic
     * @param authorName the name of the publisher creating the topic
     */
    public void handleCreateTopic(String topicID, String topicName,String authorName) {
        if (!topicMap.containsKey(topicID)) {
            Topic topic = new Topic(topicID, topicName, authorName);  // create new topic name
            topicMap.put(topicID, topic);  
            System.out.println("Topic created by " + authorName + " " + topic);
        } else {
            System.out.println("Topic already exists with ID: " + topicID);
        }
    }
    
    
  
    
    public String getPublisherName(Socket socket) {
        return publisherNames.get(socket);  
    }

   
    /**
     * Deletes a topic by its ID.
     * 
     * @param topicID the ID of the topic to delete
     */
    public void handleDelete(String topicID) {
    	    topicMap.remove(topicID);
       	System.out.println(topicID + " successfully delete");
    	    for (SubscriberHandler connection: subscriberConnections) {
    		    connection.removeTopic(topicID);
      	}
    }
    
    /**
     * Deletes a topic if the request comes from the topic's author.
     * 
     * @param topicID  the ID of the topic to delete
     * @param authorName the name of the author attempting to delete the topic
     * @param out      the PrintWriter for sending responses to the client
     * @return true if the topic was successfully deleted, false otherwise
     */
    public boolean handleDelete(String topicID, String authorName, PrintWriter out) {
    	    if (topicMap.containsKey(topicID)) {
    		    if (topicMap.get(topicID).getAuthorName().equals(authorName)) {
    			    topicMap.remove(topicID);
    	    	        System.out.println(topicID + " successfully delete");
    	         	for (SubscriberHandler connection: subscriberConnections) {
    	    		    connection.removeTopic(topicID);
    	       	    }
    	    	    return true;
    		    }
    		    else {
    		    	out.println("error: This topic ID is not belong to you.");
        			return false;
    		    }
    		}
    	    else {
    	    	out.println("error: This topic is not exists.");
    		    return false;
      	} 
    	
    	
    }
    
    
    
    public void handleMessage(String message) {
        System.out.println("Handling message: " + message);
    }
    
    /**
     * The main method that initializes the Broker and starts the service.
     * It registers the Broker with the Directory Service and allows the user
     * to input messages from the console to broadcast to other Brokers.
     * 
     * @param args the command-line arguments: args[0] is the port number, args[1] is the Directory Service IP address, and args[2] is the Directory Service port number
     */
    public static void main(String[] args) {
        // get port number and directoryService IP and port;
    	    int port = Integer.parseInt(args[0]);
        String directoryServiceIP = args[1];
        int directoryServicePort = Integer.parseInt(args[2]);
        
        Broker broker = new Broker(port);
        broker.startBroker(directoryServiceIP, directoryServicePort);
    }
    
    /**
     * Registers the Broker with a Directory Service and retrieves a list of other Brokers.
     * 
     * @param directoryServiceIP   the IP address of the Directory Service
     * @param directoryServicePort the port of the Directory Service
     * @return a list of strings containing information about other Brokers
     */
    public List<String> registerWithDirectoryService(String directoryServiceIP, int directoryServicePort) {
        List<String> brokerList = new ArrayList<>();
        try {
            Socket directorySocket = new Socket(directoryServiceIP, directoryServicePort);
            PrintWriter out = new PrintWriter(directorySocket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(directorySocket.getInputStream()));

     
            out.println("register," + InetAddress.getLocalHost().getHostAddress() + "," + port);
            String brokerInfo;
            String info = in.readLine();
            System.out.println(info);
            
            while ((brokerInfo = in.readLine()) != null) {
                brokerList.add(brokerInfo);
                String[] part = brokerInfo.split(",", 3);
                System.out.println(part[0] + " " + part[1] + " " + part[2]);
            }

            directorySocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return brokerList;
    }
    
    /**
     * Handles the disconnection of a Publisher, cleaning up any topics created by the Publisher.
     * 
     * @param handler the PublisherHandler associated with the Publisher
     */
    public void handlePublisherDisconnect(PublisherHandler handler) {
        String publisherName = handler.getName();
        System.out.println("Cleaning up topics for publisher: " + publisherName);
        Iterator<Map.Entry<String, Topic>> iterator = topicMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Topic> entry = iterator.next();
            String topicID = entry.getKey();
            String authorName = entry.getValue().getAuthorName();
            if (publisherName.equals(authorName)) {
                iterator.remove();  
                System.out.println("Topic " + topicID + " removed.");
                String message = "delete," + topicID;
                broadcastToOtherBrokers(message);
            }
        }
        publisherNames.values().remove(publisherName);
    }
    
    /**
     * Connects to existing Brokers based on a list of Broker information received from the Directory Service.
     * 
     * @param brokerList a list of strings containing information about Brokers
     */
    public void handlePublisherDisconnect(String name) {
        System.out.println("Cleaning up topics for publisher: " + name);

        
        Iterator<Map.Entry<String, Topic>> iterator = topicMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Topic> entry = iterator.next();
            String topicID = entry.getKey();
            String authorName = entry.getValue().getAuthorName();
            if (name.equals(authorName)) {
                iterator.remove();
                System.out.println("Topic " + topicID + " removed.");
                String message = "delete," + topicID;
                broadcastToOtherBrokers(message);
            }
        }

        // cancel the name of publish
        publisherNames.values().remove(name);
    }

    
    /**
     * Handles the disconnection of a Subscriber by cleaning up their subscriptions.
     * 
     * @param handler the SubscriberHandler associated with the Subscriber
     * @param name    the name of the Subscriber
     */
    public void handleSubscriberDisconnect(SubscriberHandler handler, String name) {
        // get all subscribed topic
        List<String> subscribedTopics = handler.getSubscriberTopic();
        System.out.println("Cleaning up subscriptions for subscriber.");

        for (String topicID : subscribedTopics) {
            unsubTopic(topicID,name); 
            String message = "unsub," + topicID + "," + name;
            broadcastToOtherBrokers(message);
        }
        subscriberConnections.remove(handler);
    }
    
    /**
     * Connects to a list of existing Brokers by parsing their IP addresses and ports,
     * and establishing connections.
     * 
     * @param brokerList a list of strings containing information about existing Brokers in the format "brokerName,IP,port"
     */
    public void connectToExistingBrokers(List<String> brokerList) {
    	
        for (int i=0; i<brokerList.size(); i++) {
          	String brokerInfo = brokerList.get(i);
            String[] brokerDetails = brokerInfo.split(",");
            String brokerIP = brokerDetails[1];
            int brokerPort = Integer.parseInt(brokerDetails[2]);
            connectToBroker(brokerIP, brokerPort);
        }
    }
    
    
    /**
     * Retrieves the Topic object associated with a given topic ID.
     * 
     * @param topicID the ID of the topic
     * @return the Topic object, or null if the topic does not exist
     */
    public Topic getTopic(String topicID) {
    	    return topicMap.get(topicID);
    }
    
    
 
}
