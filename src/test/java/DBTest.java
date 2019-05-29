import com.DBLinkPool.ConnectionPool;
import org.apache.spark.sql.ColumnName;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

public class DBTest {
    public static void main(String[] args) throws SQLException {
        //读取数据
        String inputData = "输出的数据";
        //数据处理
        String outputData = calculate(inputData);

        //数据保存
        //1、保存本地文件

        //2、保存到数据库
        Connection conn = ConnectionPool.getConnection();
        PreparedStatement st = conn.prepareStatement("insert into tableName(name) values("+"outputData)");
        st.execute();
        st.close();
        conn.close();
    }

    public static String calculate(String input){
        return (input + "处理数据");
    }
}
