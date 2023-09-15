package flink;

import java.io.File;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.formats.csv.*;

public class SourceAccountData {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
   //     final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        
        CsvReaderFormat<SourceAccount> csvFormat = CsvReaderFormat.forPojo(SourceAccount.class);
        
        String filesPath="/Users/administrator/data/test.csv";
        
        File file = new File(filesPath);
        
        FileSource<SourceAccount> source =
                FileSource.forRecordStreamFormat(csvFormat, Path.fromLocalFile(file)).build();
        
        final DataStream<SourceAccount> stream =
        		  env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");
        stream.print();
        env.execute();
        
}
}


