package per.owisho.learn.tools;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class ClientEventSocket {
    @Test
    public void createClientEventSocketServer() throws IOException, InterruptedException {
        ServerSocket serverSocket = new ServerSocket(9999);
        Socket socket = serverSocket.accept();
        String fileName = "/Users/owisho/Desktop/tmp/tmp_100.txt";
        BufferedReader reader = new BufferedReader(new FileReader(fileName));
        String line;
        int cnt = 0;
        long maxTimeStamp = 0;
        while ((line = reader.readLine()) != null) {
            cnt++;
            OutputStream os = socket.getOutputStream();
            Thread.sleep(1000);
            os.write((line + "\n").getBytes());
            os.flush();
            System.out.println(line);
        }
        System.out.println(cnt);
    }
}