package directoryService;



import java.io.*;
import java.net.*;
import java.util.*;

/**
 * The DirectoryServiceHandler class handles incoming requests from Brokers, Publishers, or Subscribers 
 * on behalf of the DirectoryService. It manages requests to register new Brokers and queries for 
 * available Brokers. This class implements the Runnable interface, allowing it to run in a separate thread for 
 * handling each client connection.
 * 
 * @author Hanzhou Fang
 * student id:1166053
 */
public class DirectoryServiceHandler implements Runnable{
	private Socket socket;
    private DirectoryService directoryService;

    /**
     * Constructs a DirectoryServiceHandler to handle the connection between a client and the DirectoryService.
     * 
     * @param socket the socket representing the connection to the client
     * @param directoryService the DirectoryService managing the list of Brokers
     */
    public DirectoryServiceHandler(Socket socket, DirectoryService directoryService) {
        this.socket = socket;
        this.directoryService = directoryService;
    }
    
    /**
     * Handles the incoming requests from clients, which can be either Broker registration or queries for
     * available Brokers. This method runs in a separate thread for concurrent request handling.
     */
    public void run() {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

            String request = in.readLine();
            String[] parts = request.split(",");

            if (parts[0].equals("register")) {
            	System.out.println("new broker come in");
                String IPAddress = parts[1];
                int port = Integer.parseInt(parts[2]);
                Map<Integer,String[]> brokerList = directoryService.getBrokerList();
                
                if (brokerList.isEmpty()) {
                	out.println("There is no other brokers right now");
                }
                else {
                	out.println("Available broker:");
                	for (Map.Entry<Integer, String[]> entry : brokerList.entrySet()) {
                    	Integer key = entry.getKey();
                        String[] value = entry.getValue();
                        out.println(key + "," + String.join(",", value));
                    }
                }
                directoryService.registNewBroker(IPAddress, port);
                
            } else if (parts[0].equals("query")) {  
            	Map<Integer,String[]> brokerList = directoryService.getBrokerList();
                for (Map.Entry<Integer, String[]> entry : brokerList.entrySet()) {
                	Integer key = entry.getKey();
                    String[] value = entry.getValue();
                    out.println(String.join(",", value));
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
