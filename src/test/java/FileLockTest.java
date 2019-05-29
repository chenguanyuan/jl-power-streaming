import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.concurrent.TimeUnit;

public class FileLockTest {
    public static void main(String[] args) throws IOException, InterruptedException {
        File file = new File("C:\\Users\\chengy\\Desktop\\bw.txt");
        RandomAccessFile rf = new RandomAccessFile(file,"rw");
        FileChannel fc = rf.getChannel();
        // 试图获取对此通道的文件的独占锁定
        // 如果由于另一个程序保持着一个重叠锁定而无法获取锁定，则返回 null
        FileLock fl = fc.tryLock();
        TimeUnit.SECONDS.sleep(10);
        if(fl == null){
            fc.close();
            rf.close();
            System.out.println("文件正在写");
        }else{
            System.out.println("没有写文件");
        }
        fl.release();
        fc.close();
        rf.close();
    }
}
