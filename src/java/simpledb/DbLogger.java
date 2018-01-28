package simpledb;

/**
 * Created by lincongli on 1/26/18.
 */
//import java.io.IOException;
//import java.util.logging.Logger;
//import java.util.logging.FileHandler;

import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by lincongli on 1/26/18.
 */
public class DbLogger {
    private String className;
    private boolean on;
    private FileWriter aWriter;

    DbLogger(String className, String path, boolean on){
        if(!on)return;
        this.on = on;
        this.className = className;
        String logFilePath = "./src/log/" + path;
        try {
            aWriter = new FileWriter(logFilePath, true);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Can not create log file!");
            System.exit(1);
        }
    }

    public void log(String msg){
        if(on) {
            try {
                aWriter.write(className + ": " + msg + "\n");
                aWriter.flush();

            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("Write to log file failed");
                System.exit(1);
            }
        }
    }
}
