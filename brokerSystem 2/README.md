In my system, I use the directory service, so the command line to start each components are different:
We first need to start directory service, the command line is: java -jar directoryservice.jar port
Then we can start with the brokers, the command line is: java -jar broker.jar port directoryservice_IP directoryservice_port
Then we start the subscriber and publisher, the command line is: java -jar subscriber.jar username directoryservice_IP directoryservice_port
java -jar publisher.jar username directoryservice_IP directoryservice_port

The system then will ask you to choose an available broker.