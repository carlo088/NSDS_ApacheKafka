package it.polimi.nsds.kafka.UserService;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.Integer.parseInt;

public class OnlineServices {
    // define a pool of threads to manage multiple client connections
    private static final ExecutorService executor = Executors.newFixedThreadPool(128);

    public static void main(String[] args) throws IOException{
        // if there are arguments use for the port of socket connection, otherwise set the default
        int port = args.length > 0 ? parseInt(args[0]) : 7268;

        // set the server socket
        ServerSocket serverSocket = new ServerSocket(port);

        //TODO: start all services here
        UserService userService = new UserService(new HashMap<>());

        System.out.println("OnlineServices listening on port: " + port);
        while(true){
            try {
                // accept a socket and run a thread for that client connection
                Socket socket = serverSocket.accept();
                //TODO: pass to connection the classes of services
                Connection connection = new Connection(socket, userService);
                executor.submit(connection);
                System.out.println("New connection established");
            } catch (IOException e){
                System.err.println("Connection error!");
            }
        }
    }

}
