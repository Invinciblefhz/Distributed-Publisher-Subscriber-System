package publisher;


import java.io.*;
import java.net.*;
import java.util.*;

/**
 * The Publisher class allows a user to act as a publisher in a distributed publish-subscribe system.
 * It enables the publisher to create topics, publish messages, delete topics, and view the number of subscribers.
 * The publisher connects to a Broker and communicates commands through a socket connection.
 * 
 * @author Hanzhou Fang
 * student id:1166053
 */
public class Publisher {
	private Socket socket;
	private PrintWriter out;
    private BufferedReader in;
    private String name;
	
    /**
     * Constructs a Publisher object and connects to a Broker selected from the Directory Service.
     * 
     * @param authorName          the name of the publisher (author)
     * @param directoryServiceIP  the IP address of the Directory Service
     * @param directoryServicePort the port number of the Directory Service
     */
    public Publisher(String authorName, String directoryServiceIP, int directoryServicePort) {
        try {
        	int choice;
        	name = authorName;
        	List<String> brokerList = queryDirectoryService(directoryServiceIP, directoryServicePort);
        	
        	// If the broker list is empty we end the function
        	if (brokerList.isEmpty()) {
                System.out.println("No brokers available.");
                return;
            }
        	
        	// Print all available brokers
        	System.out.println("Available Brokers:");
            for (int i = 0; i < brokerList.size(); i++) {
                System.out.println((i + 1) + ": " + brokerList.get(i));
            }
        	
            
            Scanner keyboard = new Scanner(System.in);
            while (true) {
            	System.out.println("Please select a number of broker to connect:");
                choice = keyboard.nextInt();
                keyboard.nextLine();  // Consume newline

                if (choice < 1 || choice > brokerList.size()) {
                    System.out.println("Invalid choice.");
                }
                else {
                	break;
                }
            	
            }
            

            String selectedBroker = brokerList.get(choice - 1);
            String[] brokerDetails = selectedBroker.split(",");
            String brokerIP = brokerDetails[0];
            int brokerPort = Integer.parseInt(brokerDetails[1]);
    		
    		// connect to the socket
    		socket = new Socket(brokerIP, brokerPort);
            out = new PrintWriter(socket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            
            // Tell the broker that the connection is from publisher
            ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
            output.writeUTF("PUBLISHER");
            output.writeUTF(name);
            output.flush();
            // Handle shutdown and notify Broker of disconnection 
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    System.out.println("Publisher is shutting down, notifying broker...");
                    
                    out.println("disconnect");
                    out.flush();
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }));
            
            // Start a new thread to receive message
            new Thread(() -> receiveMessages()).start();
            System.out.println("Please select command: create, publish, show, delete.");
        } catch (IOException ie) {
            ie.printStackTrace();
        }
    }
    
    /**
     * Queries the Directory Service to retrieve a list of available Brokers.
     * 
     * @param directoryServiceIP   the IP address of the Directory Service
     * @param directoryServicePort the port number of the Directory Service
     * @return a list of available Brokers in the format "IP,Port"
     */
    private List<String> queryDirectoryService(String directoryServiceIP, int directoryServicePort) {
        List<String> brokerList = new ArrayList<>();
        try (Socket directorySocket = new Socket(directoryServiceIP, directoryServicePort);
             PrintWriter out = new PrintWriter(directorySocket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(directorySocket.getInputStream()))) {
            
            // request query for the directory system
            out.println("query");

            // get the broker list
            String brokerInfo;
            while ((brokerInfo = in.readLine()) != null) {
                brokerList.add(brokerInfo);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return brokerList;
    }
    
    
    /**
     * Receives and prints messages from the connected Broker.
     */
    private void receiveMessages() {
        try {
            String messageFromBroker;
            while ((messageFromBroker = in.readLine()) != null) {
                System.out.println(messageFromBroker);
            }
            
        } catch (SocketException se) {
            System.out.println("Socket closed, stop receiving messages.");
        } catch (IOException e) {
            System.err.println("Error receiving messages from broker.");
            e.printStackTrace();
        }
    }
	
	
	
	

    /**
     * Sends a request to the Broker to create a new topic.
     * 
     * @param ID   the unique ID of the topic
     * @param name the name of the topic
     */
	public void createTopic(String ID, String name) {
		out.println("create," + ID + "," + name);
		out.println(this.name);
	}
	
	/**
     * Sends a message to be published to a specific topic.
     * 
     * @param topicID the ID of the topic
     * @param message the message to publish
     */
	public void publishMessage(String topicID, String message) {
        out.println("publish," + topicID + "," + message);
    }
	
	/**
     * Sends a request to the Broker to show the number of subscribers for a specific topic.
     * 
     * @param topicID the ID of the topic
     */
	public void showSubscriberCount(String topicID) {
        out.println("show," + topicID);
	}
	
	/**
     * Sends a request to the Broker to delete a specific topic.
     * 
     * @param topicID the ID of the topic to be deleted
     */
	public void deleteTopic(String topicID) {
	    out.println("delete," + topicID);
	}
	
	
	/**
     * The main method to run the Publisher. It takes the author's name, Directory Service IP, and port as arguments.
     * 
     * @param args command-line arguments: author's name, Directory Service IP, and port
     */
	public static void main(String[] args) {
		String authorName = args[0];
		String directoryServiceIP = args[1];
		int directoryServicePort = Integer.parseInt(args[2]);
		Scanner keyboard = new Scanner(System.in);
		Publisher publisher = new Publisher(authorName, directoryServiceIP, directoryServicePort);
		
		
		while (true) {
			String commandLine = keyboard.nextLine();
			String[] parts = commandLine.split(" ", 3);
			String command = parts[0];
			switch (command) {
			    case "create": 
			    	if (parts.length >= 3) {
			    		String topicID = parts[1];
			    		String name = parts[2];
			    		publisher.createTopic(topicID, name);
			    		
                    } else {
                        System.out.println("Invalid command. Usage: create {topic_id} {topic_name}");
                    }
                    break;
			    case "publish":
                    if (parts.length >= 3) {
                        String topicID = parts[1];
                        String message = parts[2];
                        publisher.publishMessage(topicID, message);
                    } else {
                        System.out.println("Invalid command. Usage: publish {topic_id} {message}");
                    }
                    break;
                case "show":
                    if (parts.length >= 2) {
                        String topicID = parts[1];
                        publisher.showSubscriberCount(topicID);
                    } else {
                        System.out.println("Invalid command. Usage: show {topic_id}");
                    }
                    break;
                case "delete":
                    if (parts.length >= 2) {
                        String topicID = parts[1];
                        publisher.deleteTopic(topicID);
                    } else {
                        System.out.println("Invalid command. Usage: delete {topic_id}");
                    }
                    break;
                default:
                    System.out.println("Unknown command. Please use 'create', 'publish', 'show', or 'delete'.");
			}
			
		}
		

	}
	
	public String getName() {
		return name;
	}

}
