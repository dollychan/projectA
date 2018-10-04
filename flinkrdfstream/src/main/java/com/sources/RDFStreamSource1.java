package com.sources;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.PriorityQueue;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import com.datatype.RDFQuadruple;


public class RDFStreamSource1 implements SourceFunction<RDFQuadruple> {
	
	private static final long serialVersionUID = 1L;
	public static final int FIRST = 1; //used for flag
	
	private final String dataFilePath;
	private final int servingSpeed;
	
	private transient BufferedReader br;
	private transient FileReader fr;
		
	public RDFStreamSource1(String dataFilePath) {
		this(dataFilePath, 1);
	}
	
	
	public RDFStreamSource1(String dataFilePath , int servingSpeedFactor) {
		if(servingSpeedFactor < 0) {
			throw new IllegalArgumentException("Serving Speed must be positive");
		}
		this.dataFilePath = dataFilePath;
		this.servingSpeed = servingSpeedFactor;
	}
	
	

	public void run(SourceContext<RDFQuadruple> sourceContext) throws Exception {
		
		fr = new FileReader(dataFilePath);
		br = new BufferedReader(fr);
		
		String line = null;
		RDFQuadruple rdfQua ;		
		long EventTime = 0;
		int restRecordCount =  servingSpeed - 1;
		boolean fileReadFinished = false;
		long fileReadFinishedTime = 0;
		
		PriorityQueue<Tuple2<Long,Object>> emitSchedule = new PriorityQueue<>(32,
				new Comparator<Tuple2<Long,Object>>(){
					@Override
					public int compare(Tuple2<Long, Object> o1, Tuple2<Long, Object> o2) {
						return o1.f0.compareTo(o2.f0);
					}			
		});
		
		
		if(br.ready() && (line=br.readLine())!=null) {
			
			EventTime = Calendar.getInstance().getTimeInMillis();
			rdfQua = RDFQuadruple.fromString(line, EventTime);		
			emitSchedule.add(new Tuple2<Long,Object>(EventTime,rdfQua));			
			
		}else {
			return ;
		}
		
		while(!emitSchedule.isEmpty() || br.ready()) {
			
			long currentEventTime = !emitSchedule.isEmpty()?emitSchedule.peek().f0:-1;  //获得该时段的时间

			while( rdfQua!=null && (emitSchedule.isEmpty() || EventTime < currentEventTime + (long)1000)) {
				
				if(restRecordCount > 0 && (line = br.readLine())!= null ) {
					rdfQua = RDFQuadruple.fromString(line, EventTime);
					//System.out.println(rdfQua.toString());
					emitSchedule.add(new Tuple2<Long,Object>(EventTime,rdfQua));
					restRecordCount--;
				}
				else if(restRecordCount == 0) {
					//每个时间结束后，发射一个watermark
					long watermarkTime = EventTime + (long)1000;
					Watermark nextWatermark = new Watermark(watermarkTime - (long)1000);
					emitSchedule.add(new Tuple2<Long,Object>(watermarkTime,nextWatermark));
					//System.out.println("1进入队列的watermark是<"+timestampToDate(watermarkTime)+" > ,nextmark是  < "+timestampToDate(watermarkTime-10000)+" >");
					//当time=n时，已经有servingSpeed条记录进入队列。所以调整time=n+1000
                    EventTime = EventTime + (long)1000;
                    restRecordCount = servingSpeed;
				}
				else {
					rdfQua = null;
					currentEventTime = -1;  
					fileReadFinishedTime = EventTime;
					fileReadFinished = true;   //file has read over	
				}
				
			}
			//System.out.println("当前队列有消息条数: "+emitSchedule.size());
			
		    Tuple2<Long,Object> headRecord = emitSchedule.poll();
			if(headRecord.f1 instanceof RDFQuadruple) {
				RDFQuadruple emitRDF =(RDFQuadruple)headRecord.f1;
				sourceContext.collectWithTimestamp(emitRDF, getEventTime(emitRDF));
				//System.out.println("发射的记录: " +emitRDF + "时间是: "+timestampToDate(getEventTime(emitRDF)));
                //System.out.println(emitRDF.toString());
			}
		    if(headRecord.f1 instanceof Watermark) {
				Watermark emitWatermark =(Watermark)headRecord.f1;
				sourceContext.emitWatermark(emitWatermark);
				System.out.println("发射了watermark,发射的时间2：" +timestampToDate(emitWatermark.getTimestamp()));
			}

	/*		if(emitSchedule.isEmpty()){
                long realTime = Calendar.getInstance().getTimeInMillis();
                Thread.sleep(EventTime - realTime);
            }*/
		    
		    if(fileReadFinished) {
				long watermarkTime = fileReadFinishedTime + (long)1000;
				Watermark nextWatermark = new Watermark(watermarkTime - (long)1000);
				emitSchedule.add(new Tuple2<Long,Object>(watermarkTime,nextWatermark));
				//System.out.println("2进入队列的watermark是<"+timestampToDate(watermarkTime)+" > ,nextmark是  < "+timestampToDate(watermarkTime-10000)+" >");
				fileReadFinishedTime = fileReadFinishedTime +1000;
				fileReadFinished = false;
			//	Thread.sleep(1000);
		    }
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
