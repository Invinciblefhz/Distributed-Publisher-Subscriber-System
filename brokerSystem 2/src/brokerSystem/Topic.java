package brokerSystem;

import java.util.*;

/**
 * The Topic class represents a topic in a publish-subscribe system. It contains
 * information about the topic's ID, name, author, and a list of subscribers.
 * It also tracks the number of subscribers to the topic.
 * 
 * @author Hanzhou Fang
 * student id:1166053
 */
public class Topic {
	private String topicID;
    private String name;
    private String authorName;
    private int numSubscribe;
    private List<String> subscriber;

    /**
     * Constructs a Topic with the specified ID, name, and author.
     * 
     * @param topicID    the unique identifier for the topic
     * @param name       the name of the topic
     * @param authorName the name of the author (publisher) who created the topic
     */
    public Topic(String topicID, String name, String authorName) {
        this.topicID = topicID;
        this.name = name;
        this.authorName = authorName;
        numSubscribe = 0;
        subscriber = new ArrayList<>();
    }
    
    /**
     * Adds a subscriber to the topic and increments the subscription count.
     * 
     * @param name the name of the subscriber to add
     */
    public void addSub(String name) {
    	subscriber.add(name);
    	numSubscribe++;
    }
    
    /**
     * Removes a subscriber from the topic and decrements the subscription count.
     * 
     * @param name the name of the subscriber to remove
     */
    public void unsub(String name) {
    	subscriber.remove(name);
    	numSubscribe--;
    }
    
    /**
     * Returns a string that lists the topic's ID, name, and author.
     * 
     * @return a string listing the topic's ID, name, and author
     */
    public String listTopic() {
    	return topicID + " " + name + " " + authorName;
    }
    
    /**
     * Returns a string that shows the topic's ID, name, and the number of subscribers.
     * 
     * @return a string showing the topic's ID, name, and number of subscribers
     */
    public String showTopic() {
    	return topicID + " " + name + " " + Integer.toString(numSubscribe);
    }
    
    /**
     * Returns the topic's unique ID.
     * 
     * @return the topic's ID
     */
    public String getTopicID() {
        return topicID;
    }
    

    /**
     * Returns the name of the topic.
     * 
     * @return the name of the topic
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the name of the author (publisher) who created the topic.
     * 
     * @return the author's name
     */
    public String getAuthorName() {
        return authorName;
    }
    


    @Override
    public String toString() {
        return "TopicID: " + topicID + ", Name: " + name + ", Author: " + authorName;
    }

}
