package subscriber;


import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import brokerSystem.Broker;

/**
 * The Subscriber class allows a user to act as a subscriber in a distributed publish-subscribe system.
 * It enables the subscriber to connect to a Broker, subscribe to topics, list available topics, 
 * and receive messages published to subscribed topics.
 * 
 * @author Hanzhou Fang
 * student id:1166053
 */
public class Subscriber {
    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;
    private String name;

    /**
     * Constructs a Subscriber object and connects to a Broker selected from the Directory Service.
     * 
     * @param name                the name of the subscriber
     * @param directoryServiceIP   the IP address of the Directory Service
     * @param directoryServicePort the port number of the Directory Service
     */
    public Subscriber(String name,String directoryServiceIP, int directoryServicePort) {
        try {
        	int choice;
        	this.name = name;
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
            
            
            socket = new Socket(brokerIP, brokerPort);
            out = new PrintWriter(socket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            
            // tell the broker that the connection is subscriber;
            ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
            output.writeUTF("SUBSCRIBER");
            output.writeUTF(name);
            output.flush();
            
         // Use a new thread to receive message
            new Thread(() -> receiveMessages()).start();
            
            System.out.println("Please select command: list, sub, current, unsub.");
         // When the subscriber shut down we will disconnect and cancel all it subscription.
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

            
        } catch (IOException e) {
            e.printStackTrace();
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
        } 
        catch (IOException e) {
            System.err.println("Error receiving messages from broker.");
            e.printStackTrace();
        }
    }
    
    /**
     * Processes the subscriber's input commands and sends the corresponding requests to the Broker.
     * 
     * @param commandLine the command entered by the subscriber
     */
    public void dealCommand(String commandLine) {
    	String[] parts = commandLine.split(" ", 2);
        String command = parts[0];
        switch (command) {
            case "list":
            	
                if (parts.length == 2 && parts[1].equals("all")) {
                	out.println("list,all");
                } else {
                    System.out.println("Invalid command. Usage: list {all}");
                }
                break;
            case "sub":
                if (parts.length == 2) {
                    String topicID = parts[1];
                    out.println("sub," + topicID);
                    break;
                } else {
                    System.out.println("Invalid command. Usage: sub {topic_id}");
                }
                break;
            case "current":
                if (parts.length == 1) {
                	out.println("current");
                } else {
                    System.out.println("Invalid command. Usage: current");
                }
                break;
            case "unsub":
                if (parts.length == 2) {
                    String topicID = parts[1];
                    out.println("unsub," + topicID);
                } else {
                    System.out.println("Invalid command. Usage: unsub {topic_id}");
                }
                break;
            default:
                System.out.println("Unknown command. Please use 'list', 'sub', 'current', or 'unsub'.");
        }
    }

    /**
     * The main method to run the Subscriber. It takes the subscriber's name, Directory Service IP, and port as arguments.
     * 
     * @param args command-line arguments: subscriber's name, Directory Service IP, and port
     */
    public static void main(String[] args) {
    	String name = args[0];
    	String directoryServiceIP = args[1];
        int directoryServicePort = Integer.parseInt(args[2]);

        Subscriber subscriber = new Subscriber(name, directoryServiceIP, directoryServicePort);
        
        Scanner keyboard = new Scanner(System.in);

        while (true) {
            String commandLine = keyboard.nextLine();
            subscriber.dealCommand(commandLine);
            
        }
    }
}
