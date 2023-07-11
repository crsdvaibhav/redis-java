import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Redis {
    private final ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>(); //Concurrent HashMap to deal with threads

    public void handleConnection(Socket socket) throws Exception {
        
        ArrayList<String> args = new ArrayList<String>(); 
        //To store arguments
        
        BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
        //InputStreamReader converts bytes to chars according to charset. Then we use a bufferedReader to read the chars grouped by newlines
        
        OutputStreamWriter writer = new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8);
        //Writes onto the outputstream according to specified charset
        
        while (true) { //To keep the connection

            args.clear(); //Empty the array after one call

            String line = reader.readLine();
            if (line == null) break;
            //No input so stop reading

            if (line.charAt(0) != '*') throw new RuntimeException("Cannot understand arg batch: " + line);
            //Invalid input according to RESP

            int numberOfArgs = Integer.parseInt(line.substring(1));
            //Get the number of arguments

            for (int i = 0; i < numberOfArgs; i++) { //Loop over them, to parse them

                line = reader.readLine();
                if (line == null || line.charAt(0) != '$'){
                    throw new RuntimeException("Cannot understand arg length: " + line);
                }//Check for length of argument
                    
                int argLen = Integer.parseInt(line.substring(1));
                //Get the length of argument

                line = reader.readLine();
                if (line == null || line.length() != argLen){
                    throw new RuntimeException("Wrong arg length expected " + argLen + " got: " + line);
                }//Check the next argument for its length

                args.add(line.toLowerCase()); //Add it to args array
            }

            String reply = executeCommand(args); //Get the reply based on args

            if (reply == null) {
                writer.write("$-1\r\n"); //The null response

            } else if (reply == "PING"){
                writer.write("+PING\r\n"); //simple string response PING

            }else if (reply == "OK"){
                writer.write("+OK\r\n"); //Simple string response OK

            } else {
                writer.write("$" + reply.length() + "\r\n" + reply + "\r\n"); //Bulk string response

            }
            writer.flush(); //Flush the writer
        }
    }

    String executeCommand(List<String> args) {
        
        switch (args.get(0)) {

            case "ping":
                return "PONG";
                //Respond to PING

            case "echo":
                return args.get(1);
                //Respond with the message

            case "get":
                return store.containsKey(args.get(1)) ? store.get(args.get(1)) : null;
                //IF key is there then send it else null

            case "set":
                //Add the key, value pair to HashMap
                String key = args.get(1);
                String value = args.get(2);
                store.put(key, value);
                
                if(args.size()>3 && args.get(3).equals("px")){
                    
                    long expiryTime = Long.parseLong(args.get(4));
                    //Get the expiry time
                    
                    // Schedule a task to remove the key from the cache after the expiry time
                    Executors.newSingleThreadScheduledExecutor().schedule(() -> {
                        store.remove(key);
                        System.out.println("Removed from store, key: " + key);
                    }, expiryTime, TimeUnit.MILLISECONDS); //Time units is milliseconds
                    
                }
                return "OK";
                //Return OK

            default:
                throw new IllegalArgumentException("Unknown command: " + args.get(1));
                //Handle unidentified commands
        }
    }
}
