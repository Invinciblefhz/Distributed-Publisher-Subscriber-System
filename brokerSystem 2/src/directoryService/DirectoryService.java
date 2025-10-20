package directoryService;

import java.io.*;
import java.net.*;
import java.util.*;



/**
 * The DirectoryService class acts as a directory for Brokers in a publish-subscribe system. 
 * It manages the registration of Brokers and provides information to Publishers and Subscribers 
 * about available Brokers.
 * 
 * @author Hanzhou Fang
 * student id:1166053
 */
public class DirectoryService {
	private Map<Integer,String[]> brokerList = new HashMap<>();
	private int brokerID = 1;

	 /**
     * The main method starts the Directory Service by specifying the port number.
     * 
     * @param args command-line arguments where args[0] is the port number for the Directory Service
     */
	public static void main(String[] args) {
		int port = Integer.parseInt(args[0]);
		DirectoryService directoryService = new DirectoryService();
        directoryService.startDirectoryService(port);

	}
	
	
	 /**
     * Starts the Directory Service on the specified port. It listens for incoming connections
     * from Brokers, Publishers, and Subscribers. Each new connection is handled in a separate thread.
     * 
     * @param port the port number on which the Directory Service listens for connections
     */
	public void startDirectoryService(int port) {
		try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Directory Service started on port " + port);

            while (true) {
                Socket socket = serverSocket.accept();
                new Thread(new DirectoryServiceHandler(socket, this)).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
	}
	
	
	/**
     * Registers a new Broker with the Directory Service. Each Broker is assigned a unique ID 
     * and its IP address and port are stored.
     * 
     * @param IPAddress the IP address of the new Broker
     * @param port      the port number on which the Broker is listening
     */
	public void registNewBroker(String IPAddress, int port) {
		String[] brokerInfo = new String[2];
		brokerInfo[0] = IPAddress;
		brokerInfo[1] = Integer.toString(port);
		brokerList.put(brokerID, brokerInfo);
		System.out.println("New Broker: " + brokerID + " at address " + IPAddress + " port " + brokerInfo[1]);
		brokerID++;
	}
	
	public Map<Integer,String[]> getBrokerList() {
		return brokerList;
	}

}
