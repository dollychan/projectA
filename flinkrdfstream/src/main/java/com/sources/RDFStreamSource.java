package com.sources;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import com.datatype.RDFQuadruple;


public class RDFStreamSource implements SourceFunction<RDFQuadruple> {
	
	private static final long serialVersionUID = 1L;
	public static final int FIRST = 1; //used for flag
	
	private final String dataFilePath;
	private final int servingSpeed;
	
	private transient BufferedReader br;
	private transient FileReader fr;
		
	public RDFStreamSource(String dataFilePath) {
		this(dataFilePath, 1);
	}
	
	
	public RDFStreamSource(String dataFilePath , int servingSpeedFactor) {
		if(servingSpeedFactor < 0) {
			throw new IllegalArgumentException("Serving Speed must be positive");
		}
		this.dataFilePath = dataFilePath;
		this.servingSpeed = servingSpeedFactor;
	}
	
	
	public void run(SourceContext<RDFQuadruple> sourceContext) throws Exception {
		
		fr = new FileReader(dataFilePath);
		br = new BufferedReader(fr);
		int restRecordsCount;
		
		
		String line = null;
		RDFQuadruple rdfQua ;		
		restRecordsCount = servingSpeed;
		long EventTime = 0;
		int flag = 1;
		while((line=br.readLine())!=null) {
			if(restRecordsCount == servingSpeed && flag == FIRST) {
				EventTime = Calendar.getInstance().getTimeInMillis();
				rdfQua = RDFQuadruple.fromString(line,EventTime);
				//System.out.println("发射的数据first time: " + rdfQua.toString()+" 事件的时间:" + timestampToDate(getEventTime(rdfQua)));
				sourceContext.collectWithTimestamp(rdfQua, getEventTime(rdfQua));
				flag = 0;
				restRecordsCount--;
				
			}
			else {
				if(restRecordsCount>0) {
					rdfQua = RDFQuadruple.fromString(line,EventTime);
					//System.out.println("发射的数据>0: " + rdfQua.toString()+" 事件的时间:" + timestampToDate(getEventTime(rdfQua)));
					sourceContext.collectWithTimestamp(rdfQua, getEventTime(rdfQua));
					restRecordsCount--;
					
				}
				else {
					//Thread.sleep(1000);
					System.out.println("发射了watermark,发射的时间1：" +timestampToDate(EventTime));
					sourceContext.emitWatermark(new Watermark(EventTime));  //EventTime emit a watermark
					Thread.sleep(1000);
					restRecordsCount = servingSpeed;
					EventTime = EventTime + 1000;
					rdfQua = RDFQuadruple.fromString(line,EventTime);
					//System.out.println("发射的数据==serving Speed: " + rdfQua.toString()+" 事件的时间:" + timestampToDate(getEventTime(rdfQua)));
					sourceContext.collectWithTimestamp(rdfQua, getEventTime(rdfQua));
					restRecordsCount--;
									
				}
			}			
		}
		long endEmitTime = 6 ;
		while(endEmitTime  > 0) {
			System.out.println("额外发射了watermark,发射的时间2：" +timestampToDate(EventTime));
			sourceContext.emitWatermark(new Watermark(EventTime));  //EventTime emit a watermark
			Thread.sleep(1000);
			EventTime = EventTime +1000;
			endEmitTime --;
		}
		
		this.br.close();
		this.br = null;
		this.fr.close();
		this.fr = null;
	}
	

		
	
	public long getEventTime(RDFQuadruple Qrdf) {
		return Qrdf.getEventTime();
	}


	public void cancel() {
		try {
			if (this.br != null) {
				this.br.close();
			}
			if (this.fr != null) {
				this.fr.close();
			}
		} catch(IOException ioe) {
			throw new RuntimeException("Could not cancel SourceFunction", ioe);
		} finally {
			this.br = null;
			this.fr = null;
		}
		
	}
	
	public static String timestampToDate(long s) {
		String res;
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = new Date(s);
		res = simpleDateFormat.format(date);
		return res;
	}

}
