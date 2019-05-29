import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

public class readFile {
    public static void main(String[] args) throws IOException {
        File file = new File("C:\\Users\\chengy\\Desktop\\bw.txt");
        System.out.println(file.length());
        FileInputStream inputStream = new FileInputStream(file);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        String data = null;
        while((data = bufferedReader.readLine()) != null){
            System.out.println("reading......");
            System.out.println(data);
        }

    }
}
