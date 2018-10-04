package com.basic;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.datatype.RDFQuadruple;
import com.querys.ExecuteQuery;
import com.sources.RDFStreamSource;
import com.sources.RDFStreamSource1;

public class RDFStreamDemo {

	public static void main(String[] args) throws Exception {
			
		//获取运行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		//设置并行度
		env.setParallelism(1);
		
					
		//连接datasource获取输入数据
		DataStream<RDFQuadruple> rdfs = env.addSource(new RDFStreamSource1("/home/liuchen/Documents/my_rsp/impl/flinkrdfstream/src/dataset/lubm_50/University0.nt",103000));

		//rdfs.print();

		ExecuteQuery.query11(rdfs);
	  
		System.out.println("Begining...");
		env.execute("Streaming");

	}

}
