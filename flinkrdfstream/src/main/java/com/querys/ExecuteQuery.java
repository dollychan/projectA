package com.querys;

import java.util.Calendar;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.datatype.RDFQuadruple;
import com.sources.RDFStreamSource;

public class ExecuteQuery {

	/**
	 * 
	 * @ClassName: RDFStreamTransfer1
	 * @Description: query1
	 * @author hepengyu
	 * @date 2018年9月15日 下午5:48:46
	 *  SELECT ?X	
		WHERE
		{ 
			?X rdf:type ub:UnderGraduateStudent .
		    ?X ub:takesCourse http://www.Department0.University0.edu/Course49
		}
	 */
	public static void query1(DataStream<RDFQuadruple> rdfs) {
		// str_normal correspondence
		// query1
		DataStream<RDFQuadruple> filter1 = rdfs.filter(new FilterFunction<RDFQuadruple>() {
			@Override
			public boolean filter(RDFQuadruple value) throws Exception {
				// 26
				if ((value.getPredicate().equals(RDFTerm.type)
						&& value.getObject().equals(RDFTerm.undergradStu)) ||
                        (value.getPredicate().equals(RDFTerm.takeCourse)
						&& value.getObject().equals(RDFTerm.Univ0Course0))) {
					return true;
				}
				return false;
			}
		});

		//filter1.print();

		// transfer quadruple with timestamp to triple
		DataStream<String> result = filter1.map(new MapFunction<RDFQuadruple, Tuple3<String, String, String>>() {
			@Override
			public Tuple3<String, String, String> map(RDFQuadruple value) throws Exception {
				return new Tuple3<>(value.getSubject(), value.getPredicate(),
						value.getObject());
			}
		}).keyBy(0) // partition by subject
				// * * * * *
				// * * * * *
				// * * * * *
				// window size is 1 seconds and moved 1 per second
				// build sliding window
			.window(SlidingEventTimeWindows.of(Time.seconds(1), Time.seconds(1)))
			// count similar subjects in window
			.apply(new WindowFunction<Tuple3<String, String, String>, String, Tuple, TimeWindow>() {
				@Override
				public void apply(Tuple key, TimeWindow window, Iterable<Tuple3<String, String, String>> values,
						Collector<String> out) throws Exception {
					String subject = null;
					boolean bool1 = false;
					boolean bool2 = false;
					for (Tuple3<String, String, String> v : values) {
						subject = v.f0;
						if (v.f1.equals(RDFTerm.type) && v.f2.equals(RDFTerm.undergradStu)) {
							bool1 = true;
						}
						if ((v.f1.equals(RDFTerm.takeCourse)
								&& v.f2.equals(RDFTerm.Univ0Course0))) {
							bool2 = true;
						}
						if (bool1 && bool2) {
							System.out.println(subject);
							out.collect(subject);
						}
					}
				}
			});

		result.print();

	}
	
	/**
	 * 
	 * @ClassName: RDFStreamTransfer2 
	 * @Description: To-DO query2 undone
	 * @author hepengyu
	 * @date 2018年9月15日 下午8:52:23 
	 *
	 *  SELECT ?X, ?Y, ?Z
		WHERE
		{        1  		 25
		  ?X rdf:type ub:GraduateStudent .
		  		 1 			 4
		  ?Y rdf:type ub:University .
		  		 1			 6
		  ?Z rdf:type ub:Department .
		  		 22
		  ?X ub:memberOf ?Z .
		  		 7
		  ?Z ub:subOrganizationOf ?Y .
		  		 10
		  ?X ub:undergraduateDegreeFrom ?Y
		}
	 */
	public static void query2(DataStream<RDFQuadruple> rdfs) {
//		?Y rdf:type ub:University .
			DataStream<String> firstFilterY = rdfs.filter(new FilterFunction<RDFQuadruple>() {
				@Override
				public boolean filter(RDFQuadruple value) throws Exception {
					if(value.getPredicate().equals(RDFTerm.type) && value.getObject().equals(RDFTerm.university)) {
						return true;
					}
					return false;
				}
			}).map(new MapFunction<RDFQuadruple, String>() {
				@Override
				public String map(RDFQuadruple value) throws Exception {
					return String.valueOf(value.getSubject());
				}
			});

			//firstFilterY.print();
			
			//  ?Z rdf:type ub:Department .
			//  ?Z ub:subOrganizationOf ?Y .
			DataStream<Tuple2<String, String>> zTupleWithY = rdfs.filter(new FilterFunction<RDFQuadruple>() {
				@Override
				public boolean filter(RDFQuadruple value) throws Exception {
					if((value.getPredicate().equals(RDFTerm.type) && value.getObject().equals(RDFTerm.department))
                            || (value.getPredicate().equals(RDFTerm.subOrganOf))) {
						return true;
					}
					return false;
				}
			}).map(new MapFunction<RDFQuadruple, Tuple3<String,String,String>>() {
				@Override
				public Tuple3<String, String, String> map(RDFQuadruple value) throws Exception {
					return new Tuple3<>(value.getSubject().toString(), value.getPredicate().toString(),value.getObject().toString());
				}
			}).keyBy(0)
                    .window(SlidingEventTimeWindows.of(Time.seconds(1), Time.seconds(1)))
			        .apply(new WindowFunction<Tuple3<String,String,String>, Tuple2<String,String>, Tuple, TimeWindow>() {
				@Override
				public void apply(Tuple key, TimeWindow window, Iterable<Tuple3<String, String, String>> values,
						Collector<Tuple2<String, String>> out) throws Exception {
					String subject = null;
					String predicate = null;
					String object = null;
					String obj1 = null;
					boolean bool1 = false;
					boolean bool2 = false;
					for (Tuple3<String, String, String> v : values) {

						if(v.f1.equals(RDFTerm.type) && v.f2.equals(RDFTerm.department)) {
                            subject = v.f0;
							bool1 = true;
						}
						if(v.f1.equals(RDFTerm.subOrganOf)) {
							obj1 = v.f2;
							bool2 = true;
						}
						if(bool1 && bool2) {
							out.collect(new Tuple2<>(subject,obj1));
						}
					}
				}
			});

			//zTupleWithY.print();
			
			// OK
//			?X rdf:type ub:GraduateStudent .
//			?X ub:memberOf ?Z .
//			?X ub:undergraduateDegreeFrom ?Y
			DataStream<Tuple3<String, String, String>> wholeFilterX = rdfs.filter(new FilterFunction<RDFQuadruple>() {
				@Override
				public boolean filter(RDFQuadruple value) throws Exception {
					if((value.getPredicate().equals(RDFTerm.type) && value.getObject().equals(RDFTerm.gradStu))
							|| (value.getPredicate().equals(RDFTerm.memberOf))
                            || (value.getPredicate().equals(RDFTerm.undergradDegreeFrom) || value.getPredicate().equals(RDFTerm.doctoralDegreeFrom) || value.getPredicate().equals(RDFTerm.masterDegreeFrom))) {
						return true;
					}
					return false;
				}
			}).map(new MapFunction<RDFQuadruple, Tuple3<String,String,String>>() {
				@Override
				public Tuple3<String, String, String> map(RDFQuadruple value) throws Exception {
					return new Tuple3<>(value.getSubject().toString(), value.getPredicate().toString(),value.getObject().toString());
				}
			});
			
			
			// XResult  <GradStu, memberof Depart, underDegreeFrom Univ>
			DataStream<Tuple3<String, String, String>> xYTuple = wholeFilterX.keyBy(0)
                    .window(SlidingEventTimeWindows.of(Time.seconds(1), Time.seconds(1)))
			.apply(new WindowFunction<Tuple3<String,String,String>, Tuple3<String,String,String>, Tuple, TimeWindow>() {
				@Override
				public void apply(Tuple key, TimeWindow window, Iterable<Tuple3<String, String, String>> values,
						Collector<Tuple3<String, String, String>> out) throws Exception {
					String subject = null;
					String obj1 = null;
					String obj2 = null;
					boolean bool1 = false;
					boolean bool2 = false;
					for (Tuple3<String, String, String> v : values) {

						if(v.f1.equals(RDFTerm.type) && v.f2.equals(RDFTerm.gradStu) ) {
                            subject = v.f0;
							bool1 = true;
						}
						// ?Z
						if(v.f1.equals(RDFTerm.memberOf)) {
							obj1 = v.f2;
							bool2 = true;
						}
					}
                    if(!bool1 || !bool2)
                        return;

                    for (Tuple3<String, String, String> v : values) {
                        if(v.f1.equals(RDFTerm.undergradDegreeFrom) || v.f1.equals(RDFTerm.masterDegreeFrom) || v.f1.equals(RDFTerm.doctoralDegreeFrom)) {
                            obj2 = v.f2;
                            //System.out.println(subject + ", " + obj1 + ", "  + v.f1 + ", " + obj2);
                            out.collect(new Tuple3<>(subject, obj1, obj2));
                        }
                    }
				}
			}).join(firstFilterY).where(new KeySelector<Tuple3<String,String,String>, String>() {
				// subject predicate object obj1:Z obj2:Y
				@Override
				public String getKey(Tuple3<String, String, String> value) throws Exception {
					return value.f2;
				}
			}).equalTo(new KeySelector<String, String>() {
				@Override
				public String getKey(String value) throws Exception {
					return value;
				}
			}).window(SlidingEventTimeWindows.of(Time.seconds(1), Time.seconds(1)))
			.apply(new JoinFunction<Tuple3<String,String,String>, String, Tuple3<String,String,String>>() {
				@Override
				public Tuple3<String, String, String> join(Tuple3<String, String, String> first,
						String second) throws Exception {
					return new Tuple3<>(first.f0, first.f1, second);
				}
			});

			//xYTuple.print();
			
			//<GadStudent, memberof Depart, degreeFrom Univ, Depart subOrgan Univ>
			DataStream<Tuple4<String, String, String, String>> xYZTuple = xYTuple.join(zTupleWithY)
                    .where(new KeySelector<Tuple3<String,String,String>,String>() {
				@Override
				public String getKey(Tuple3<String, String, String> value) throws Exception {
					return value.f1;
				}
			}).equalTo(new KeySelector<Tuple2<String,String>, String>() {
				@Override
				public String getKey(Tuple2<String,String> value) throws Exception {
					return value.f0;
				}
			}).window(SlidingEventTimeWindows.of(Time.seconds(1), Time.seconds(1)))
				.apply(new JoinFunction<Tuple3<String,String,String>, Tuple2<String,String>, Tuple4<String,String,String, String>>() {
					@Override
					public Tuple4<String, String,String, String> join(Tuple3<String, String, String> first,
							Tuple2<String, String> second) throws Exception {
						// ?X ?Z
						return new Tuple4<>(first.f0, first.f1, first.f2, second.f1);
					}
				});

			//xYZTuple.print();

			xYZTuple.filter(new FilterFunction<Tuple4<String, String, String, String>>() {
                @Override
                public boolean filter(Tuple4<String, String, String, String> value) throws Exception {
                    if(value.f2.equals(value.f3))
                        return true;
                    else return false;
                }
            }).print();
	}
	
	/**
	 * 
	 * @ClassName: RDFStreamTransfer3
	 * @Description: query3
	 * @author hepengyu
	 * @date 2018年9月15日 下午11:03:45
	 *
	 *       SELECT ?X
	 *       WHERE
	 *       { 
	 *       			1 			27 
	 *       	?X rdf:type 	ub:Publication . 
	 *       			28 						1301091 
	 *       	?X ub:publicationAuthor  http://www.Department0.University0.edu/AssistantProfessor0
	 *       }
	 */
	public static void query3(DataStream<RDFQuadruple> rdfs) {
		rdfs.filter(new FilterFunction<RDFQuadruple>() {
			@Override
			public boolean filter(RDFQuadruple value) throws Exception {
				if ((value.getPredicate().equals(RDFTerm.type) && value.getObject().equals(RDFTerm.publication))
						|| (value.getPredicate().equals(RDFTerm.publicationAuthor) && value.getObject().equals(RDFTerm.Univ0AssiProf0))) {
					return true;
				}
				return false;
			}
		}).map(new MapFunction<RDFQuadruple, Tuple3<String, String, String>>() {
			@Override
			public Tuple3<String, String, String> map(RDFQuadruple value) throws Exception {
				return new Tuple3<>(value.getSubject().toString(), value.getPredicate().toString(),
						value.getObject().toString());
			}
		}).keyBy(0).window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1)))
			.apply(new WindowFunction<Tuple3<String, String, String>, String, Tuple, TimeWindow>() {
				@Override
				public void apply(Tuple key, TimeWindow window, Iterable<Tuple3<String, String, String>> values,
						Collector<String> out) throws Exception {
					String subject = null;
					boolean bool1 = false;
					boolean bool2 = false;
					for (Tuple3<String, String, String> v : values) {
						subject = v.f0;
						if (v.f1.equals(RDFTerm.type) && v.f2.equals(RDFTerm.publication)) {
							bool1 = true;
						}
						if ((v.f1.equals(RDFTerm.publicationAuthor) && v.f2.equals(RDFTerm.Univ0AssiProf0))) {
							bool2 = true;
						}
						if (bool1 && bool2) {
							out.collect(subject);
						}
					}
				}
			}).print();
	}

	/**
	 * 
	 * @ClassName: RDFStreamTransfer4 
	 * @Description: query4
	 * @author hepengyu
	 * @date 2018年9月16日 上午12:02:11 
	 *
	 *
	 *  SELECT ?X, ?Y1, ?Y2, ?Y3
		WHERE	
		{		1		
		  ?X rdf:type ub:Professor .	fullProfessor 8 and associate professor 18 and Assistant Professor 19
		  		13							1300965	
		  ?X ub:worksFor <http://www.Department0.University0.edu> .
		  		5		
		  ?X ub:name ?Y1 .
		  		14
		  ?X ub:emailAddress ?Y2 .
		  		15
		  ?X ub:telephone ?Y3
		}
	 */
	public static void query4(DataStream<RDFQuadruple> rdfs) {
			// ?X rdf:type ub:Professor 
			// ?X ub:worksFor <http://www.Department0.University0.edu> 
			DataStream<Tuple3<String, String, String>> filter1 = rdfs.filter(new FilterFunction<RDFQuadruple>() {
				@Override
				public boolean filter(RDFQuadruple value) throws Exception {
					// Professor
					// ?X rdf:type ub:Professor
					if ((value.getPredicate().equals(RDFTerm.type)
							&& (value.getObject().equals(RDFTerm.fullProf) || value.getObject().equals(RDFTerm.assoProf)
									|| value.getObject().equals(RDFTerm.assiProf)))
							|| (value.getPredicate().equals(RDFTerm.worksFor) && value.getObject().equals(RDFTerm.Univ0Depart0))
							|| value.getPredicate().equals(RDFTerm.name) || value.getPredicate().equals(RDFTerm.emailAddress)
									|| value.getPredicate().equals(RDFTerm.telephone)) {
						return true;
					}
					return false;
				}
			}).map(new MapFunction<RDFQuadruple, Tuple3<String,String,String>>() {
				@Override
				public Tuple3<String, String, String> map(RDFQuadruple value) throws Exception {
					return new Tuple3<>(value.getSubject(), value.getPredicate(),
							value.getObject());
				}
			});
				
				// filter is the whole data
				// ?X ub:name ?Y1 . 
				// ?X ub:emailAddress ?Y2 .
				// ?X ub:telephone ?Y3
				// ?x result
				
				DataStream<Tuple4<String,String, String, String>> XResult = filter1.keyBy(0)
                        .window(SlidingEventTimeWindows.of(Time.seconds(1), Time.seconds(1)))
				.apply(new WindowFunction<Tuple3<String, String, String>, Tuple4<String,String,String,String>, Tuple, TimeWindow>() {
					@Override
					public void apply(Tuple key, TimeWindow window, Iterable<Tuple3<String, String, String>> values,
							Collector<Tuple4<String,String,String,String>> out) throws Exception {
						String subject = null, name = null, email = null, telephone = null;
						boolean bool1 = false;
						boolean bool2 = false;
						boolean bool3 = false;
						boolean bool4 = false;
						boolean bool5 = false;
						for (Tuple3<String, String, String> v : values) {
							if(v.f1.equals(RDFTerm.type) && (v.f2.equals(RDFTerm.fullProf) || v.f2.equals(RDFTerm.assoProf) || v.f2.equals(RDFTerm.assiProf))) {
                                subject  = v.f0;
							    bool1 = true;
							}
							
							if(v.f1.equals(RDFTerm.worksFor) && v.f2.equals(RDFTerm.Univ0Depart0)) {
								bool2 = true;
							}
							
							if(v.f1.equals(RDFTerm.name)) {
								bool3 = true;
								name = v.f2;
							}
							
							if(v.f1.equals(RDFTerm.emailAddress)) {
								bool4 = true;
								email = v.f2;
							}
							
							if(v.f1.equals(RDFTerm.telephone)) {
								bool5 = true;
								telephone = v.f2;
							}
							
							if(bool1 && bool2 && bool3 && bool4 && bool5) {
								out.collect(new Tuple4<>(subject,name, email, telephone));
							}
						}
					}
				});

		        XResult.print();
	}

	/**
	 * 
	 * @ClassName: RDFStreamTransfer5 undo
	 * @Description: query5
	 * @author hepengyu
	 * @date 2018年9月15日 下午11:03:45
	 *
	 *
	 *
	 *
	 *SELECT ?X
		WHERE
		{			1		
			?X rdf:type ub:Person .(FullProfessor 8 AssociateProfessor 18 
									AssistantProfessor 19 UndergraduateStudent 21
									Lecturer 20 advisor 24 GraduateStudent 25 publicationAuthor 28
									TeachingAssistant 31 ResearchAssistant 33)
					22					1300965	
	  		?X ub:memberOf <http://www.Department0.University0.edu>
	  	}
	 */
	public static void query5(DataStream<RDFQuadruple> rdfs) {
		DataStream<Tuple3<String, String, String>> endData = rdfs.filter(new FilterFunction<RDFQuadruple>() {
			@Override
			public boolean filter(RDFQuadruple value) throws Exception {
				if ((value.getPredicate().equals(RDFTerm.type) && (value.getObject().equals(RDFTerm.fullProf)
						|| value.getObject().equals(RDFTerm.assoProf) || value.getObject().equals(RDFTerm.assiProf)
						|| value.getObject().equals(RDFTerm.lecturer) || value.getObject().equals(RDFTerm.undergradStu)
						|| value.getObject().equals(RDFTerm.advisor) || value.getObject().equals(RDFTerm.gradStu)
						|| value.getObject().equals(RDFTerm.publicationAuthor) || value.getObject().equals(RDFTerm.TA)
						|| value.getObject().equals(RDFTerm.RA)))
                        || (value.getPredicate().equals(RDFTerm.memberOf) && value.getObject().equals(RDFTerm.Univ0Depart0))) {
					return true;
				}
				return false;
			}
		}).map(new MapFunction<RDFQuadruple, Tuple3<String, String, String>>() {
			@Override
			public Tuple3<String, String, String> map(RDFQuadruple value) throws Exception {
				return new Tuple3<>(value.getSubject().toString(), value.getPredicate().toString(),
						value.getObject().toString());
			}
		});
		
		DataStream<String> result = endData.keyBy(0).window(SlidingEventTimeWindows.of(Time.seconds(1), Time.seconds(1)))
			.apply(new WindowFunction<Tuple3<String, String, String>, String, Tuple, TimeWindow>() {
				@Override
				public void apply(Tuple key, TimeWindow window, Iterable<Tuple3<String, String, String>> values,
						Collector<String> out) throws Exception {
					String subject = null;
					boolean bool1 = false;
					boolean bool2 = false;
					for (Tuple3<String, String, String> v : values) {
						subject = v.f0;
						if (v.f1.equals(RDFTerm.type) && (v.f2.equals(RDFTerm.fullProf)
								|| v.f2.equals(RDFTerm.assoProf) || v.f2.equals(RDFTerm.assiProf)
								|| v.f2.equals(RDFTerm.lecturer) || v.f2.equals(RDFTerm.undergradStu)
								|| v.f2.equals(RDFTerm.advisor) || v.f2.equals(RDFTerm.gradStu)
								|| v.f2.equals(RDFTerm.publicationAuthor) || v.f2.equals(RDFTerm.TA)
								|| v.f2.equals(RDFTerm.RA))) {
							bool1 = true;
						}
						if (v.f1.equals(RDFTerm.memberOf) && v.f2.equals(RDFTerm.Univ0Depart0)) {
							bool2 = true;
						}
						if (bool1 && bool2) {
							System.err.println(subject);
							out.collect(subject);
						}
					}
				}
			});

		result.print();
	}
	
	/**
	 * 
	 * @ClassName: RDFStreamTransfer6 
	 * @Description: query6
	 * @author hepengyu
	 * @date 2018年9月17日 下午1:31:31 
	 *
	 *SELECT ?X WHERE {?X rdf:type ub:Student}
	 *student : { UndergraduateStudent 21 GraduateStudent 25}
	 *
	 */
	public static void query6(DataStream<RDFQuadruple> rdfs) {
		DataStream<String> endData = rdfs.filter(new FilterFunction<RDFQuadruple>() {
			@Override
			public boolean filter(RDFQuadruple value) throws Exception {
				if (value.getPredicate().equals(RDFTerm.type) && (value.getObject().equals(RDFTerm.undergradStu)
						|| value.getObject().equals(RDFTerm.gradStu))) {
					return true;
				}
				return false;
			}
		}).map(new MapFunction<RDFQuadruple, Tuple3<String, String, String>>() {
			@Override
			public Tuple3<String, String, String> map(RDFQuadruple value) throws Exception {
				return new Tuple3<>(value.getSubject().toString(), value.getPredicate().toString(),
						value.getObject().toString());
			}
		}).keyBy(0).window(SlidingEventTimeWindows.of(Time.seconds(1), Time.seconds(1)))
		.apply(new WindowFunction<Tuple3<String, String, String>, String, Tuple, TimeWindow>() {
			@Override
			public void apply(Tuple key, TimeWindow window, Iterable<Tuple3<String, String, String>> values,
					Collector<String> out) throws Exception {
				String subject = null;
				boolean bool1 = false;
				for (Tuple3<String, String, String> v : values) {
					subject = v.f0;
					if (v.f1.equals(RDFTerm.type) && (v.f2.equals(RDFTerm.undergradStu) || v.f2.equals(RDFTerm.gradStu))) {
						bool1 = true;
					}
					if (bool1) {
						System.out.println(subject);
						out.collect(subject);
					}
				}
			}
		});
	}

	/**
	 * 
	 * @ClassName: RDFStreamTransfer7 
	 * @Description: query7
	 * @author hepengyu
	 * @date 2018年9月17日 下午8:28:59 
	 * 
	 * SELECT ?X, ?Y
		WHERE 
		{			1		21 / 25
			?X rdf:type ub:Student .
					1		29 / 30
		   ?Y rdf:type ub:Course .
		   			23
		   ?X ub:takesCourse 	?Y .
		   					1301018												9		
		   <http://www.Department0.University0.edu/AssociateProfessor0>, ub:teacherOf, 	?Y
		}
	 *
	 */
	public static void query7(DataStream<RDFQuadruple> rdfs) {
		// <http://www.Department0.University0.edu/AssociateProfessor0>, ub:teacherOf,
		// ?Y
		DataStream<Tuple3<String, String, String>> filterYResult1 = rdfs
				.filter(new FilterFunction<RDFQuadruple>() {
					@Override
					public boolean filter(RDFQuadruple value) throws Exception {
						if (value.getSubject().equals(RDFTerm.Univ0AssoProf0) && value.getPredicate().equals(RDFTerm.teacherOf)) {
							return true;
						}
						return false;
					}
				}).map(new MapFunction<RDFQuadruple, Tuple3<String, String, String>>() {
					@Override
					public Tuple3<String, String, String> map(RDFQuadruple value) throws Exception {
						return new Tuple3<>(value.getSubject().toString(), value.getPredicate().toString(),
								value.getObject().toString());
					}
				});

		//filterYResult1.print();

		// ?Y rdf:type ub:Course .
		DataStream<Tuple3<String, String, String>> filterYResult2 = rdfs
				.filter(new FilterFunction<RDFQuadruple>() {
					@Override
					public boolean filter(RDFQuadruple value) throws Exception {
						if (value.getPredicate().equals(RDFTerm.type)
								&& (value.getObject().equals(RDFTerm.course) || value.getObject().equals(RDFTerm.gradCourse))) {
							return true;
						}
						return false;
					}
				}).map(new MapFunction<RDFQuadruple, Tuple3<String, String, String>>() {
					@Override
					public Tuple3<String, String, String> map(RDFQuadruple value) throws Exception {
						return new Tuple3<>(value.getSubject().toString(), value.getPredicate().toString(),
								value.getObject().toString());
					}
				});

		DataStream<String> yResult = filterYResult1.join(filterYResult2)
				.where(new KeySelector<Tuple3<String, String, String>, String>() {
					@Override
					public String getKey(Tuple3<String, String, String> value) throws Exception {
						return value.f2;
					}
				}).equalTo(new KeySelector<Tuple3<String, String, String>, String>() {

					@Override
					public String getKey(Tuple3<String, String, String> value) throws Exception {
						return value.f0;
					}
				}).window(SlidingEventTimeWindows.of(Time.seconds(1), Time.seconds(1)))
				.apply(new JoinFunction<Tuple3<String, String, String>, Tuple3<String, String, String>, String>() {
					@Override
					public String join(Tuple3<String, String, String> first, Tuple3<String, String, String> second)
							throws Exception {
						return first.f2;
					}
				});

		// yResult.writeAsText("E:\\Flink_data\\test\\query7\\resultY.txt",WriteMode.OVERWRITE);
		// ?X rdf:type ub:Student .
		// ?X ub:takesCourse ?Y .
		// whole the ?X
		DataStream<Tuple3<String, String, String>> XFilter1 = rdfs.filter(new FilterFunction<RDFQuadruple>() {
			@Override
			public boolean filter(RDFQuadruple value) throws Exception {
				if ((value.getPredicate().equals(RDFTerm.type)
						&& (value.getObject().equals(RDFTerm.undergradStu)
						|| value.getObject().equals(RDFTerm.gradStu)))
						|| value.getPredicate().equals(RDFTerm.takeCourse)) {
					return true;
				}
				return false;
			}
		}).map(new MapFunction<RDFQuadruple, Tuple3<String, String, String>>() {
			@Override
			public Tuple3<String, String, String> map(RDFQuadruple value) throws Exception {
				return new Tuple3<>(value.getSubject().toString(), value.getPredicate().toString(),
						value.getObject().toString());
			}
		});

		DataStream<Tuple2<String, String>> xData = XFilter1.keyBy(0)
				.window(SlidingEventTimeWindows.of(Time.seconds(1), Time.seconds(1)))
				.apply(new WindowFunction<Tuple3<String, String, String>, Tuple2<String, String>, Tuple, TimeWindow>() {
					@Override
					public void apply(Tuple key, TimeWindow window, Iterable<Tuple3<String, String, String>> values,
							Collector<Tuple2<String, String>> out) throws Exception {
						String subject = null;
						String predicate = null;
						String object = null;
						String obj1 = null;
						boolean bool1 = false;
						boolean bool2 = false;
						for (Tuple3<String, String, String> v : values) {

							if (v.f1.equals(RDFTerm.type) && (v.f2.equals(RDFTerm.undergradStu)
									|| v.f2.equals(RDFTerm.gradStu))) {
                                subject = v.f0;
								bool1 = true;
							}
							if (v.f1.equals(RDFTerm.takeCourse)) {
								obj1 = v.f2;
								bool2 = true;
							}
							if (bool1 && bool2) {
								out.collect(new Tuple2<>(subject, obj1));
							}
						}
					}
				});

		xData.join(yResult).where(new KeySelector<Tuple2<String, String>, String>() {
					@Override
					public String getKey(Tuple2<String, String> value) throws Exception {
						return value.f1;
					}
				}).equalTo(new KeySelector<String, String>() {
					@Override
					public String getKey(String value) throws Exception {
						return value;
					}
				}).window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1)))
				.apply(new JoinFunction<Tuple2<String, String>, String, Tuple2<String, String>>() {
					@Override
					public Tuple2<String, String> join(Tuple2<String, String> first, String second) throws Exception {
						return new Tuple2<>(first.f0, second);
					}
				}).print();
	}
	
	/*
	 query8:
	 PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
	 PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
	 SELECT ?X, ?Y, ?Z
	 WHERE
	 {?X rdf:type ub:Student .
  	 ?Y rdf:type ub:Department .
  	 ?X ub:memberOf ?Y .
  	 ?Y ub:subOrganizationOf <http://www.University0.edu> .
  	 ?X ub:emailAddress ?Z}	
	 */	
	public static void query8(DataStream<RDFQuadruple> rdfs) {
		
		HashSet<Tuple3<String,String,String>> sets = new HashSet<Tuple3<String,String,String>>();    //去重
		HashSet<Tuple5<String,String,String,String,String>> sets1 = new HashSet<Tuple5<String,String,String,String,String>>();   //存储?z
		
		DataStream<Tuple3<String,String,String>> tuple3 = rdfs.flatMap(new FlatMapFunction<RDFQuadruple,Tuple3<String,String,String>>(){

			public void flatMap(RDFQuadruple value, Collector<Tuple3<String, String, String>> out) throws Exception {
				
				out.collect(new Tuple3(value.getSubject(),value.getPredicate(),value.getObject()));								
			}					
		});
		
		DataStream<Tuple3<String, String, String>> y = tuple3.filter(new FilterFunction<Tuple3<String,String,String>>(){
		
			public boolean filter(Tuple3<String, String, String> value) throws Exception {
				if ((value.f1.equals(RDFTerm.type) && value.f2.equals(RDFTerm.department))
                        || (value.f1.equals(RDFTerm.subOrganOf) && value.f2.equals(RDFTerm.Univ0)) )
					return true;
				else
					return false;
			}
	    	
	    })
		.keyBy(0)
		.timeWindow(Time.seconds(1),Time.seconds(1))
		.apply(new WindowFunction<Tuple3<String,String,String>,Tuple3<String,String,String>,Tuple,TimeWindow>(){

			@Override
			public void apply(Tuple key, TimeWindow window, Iterable<Tuple3<String, String, String>> input,
					Collector<Tuple3<String, String, String>> out) throws Exception {
				
				boolean condition1 = false;
				boolean condition2 = false;
				for(Tuple3<String,String,String> in: input) {
					if(in.f1.equals(RDFTerm.type) && in.f2.equals(RDFTerm.department))
						condition1 = true;	
					if(in.f1.equals(RDFTerm.subOrganOf) && in.f2.equals(RDFTerm.Univ0))
						condition2 = true;
					if(condition1 && condition2) {
						break;
					}
				}
				if(condition1 && condition2) {
					for(Tuple3<String,String,String> in: input) {
						if(in.f1.equals(RDFTerm.subOrganOf) && in.f2.equals(RDFTerm.Univ0))    //已经满足两个条件，选择其中之一就行
							sets.add(in);
					}					
					//System.out.println(RDFStreamSource.timestampToDate(Calendar.getInstance().getTimeInMillis()) + ">>");
					Iterator i = sets.iterator();
					while(i.hasNext()) {
						Object obj = i.next();
						out.collect((Tuple3<String, String, String>) obj);
						//System.out.println(obj);
						
					}
					sets.clear();
				}
			
			}			
		});
		
		DataStream<Tuple5<String,String,String,String,String>> x = tuple3.filter(new FilterFunction<Tuple3<String,String,String>>(){
			
			public boolean filter(Tuple3<String, String, String> value) throws Exception {
				if ((value.f1.equals(RDFTerm.type) && (value.f2.equals(RDFTerm.undergradStu) || value.f2.equals(RDFTerm.gradStu) ))
                        || (value.f1.equals(RDFTerm.memberOf)) || (value.f1.equals(RDFTerm.emailAddress)) )
					return true;
				else
					return false;
			}
	    	
	    })
        .keyBy(0)
		.timeWindow(Time.seconds(1),Time.seconds(1))
		.apply(new WindowFunction<Tuple3<String,String,String>,Tuple5<String,String,String,String,String>,Tuple,TimeWindow>(){

			@Override
			public void apply(Tuple key, TimeWindow window, Iterable<Tuple3<String, String, String>> input,
					Collector<Tuple5<String, String, String,String,String>> out) throws Exception {
				
				boolean condition1 = false;
				boolean condition2 = false;
				boolean condition3 = false;
				Tuple5<String,String,String,String,String> temp = new Tuple5<String,String,String,String,String>();
				for(Tuple3<String,String,String> in: input) {
					if(in.f1.equals(RDFTerm.type) && (in.f2.equals(RDFTerm.undergradStu) || in.f2.equals(RDFTerm.gradStu)))
						condition1 = true;	
					if(in.f1.equals(RDFTerm.memberOf))
						condition2 = true;
					if(in.f1.equals(RDFTerm.emailAddress))
						condition3 = true;						
				}
				if(condition1 && condition2 && condition3) {
					for(Tuple3<String,String,String> in: input) {
						if(in.f1.equals(RDFTerm.memberOf)) {	//此处是predicate=22
							temp.f0 = in.f0;
							temp.f1 = in.f1;
							temp.f2 = in.f2;
						}
						if(in.f1.equals(RDFTerm.emailAddress)) {	//此处是predicate=14
							temp.f3 = in.f1;
							temp.f4 = in.f2;
						}	
					}
					out.collect(temp);
					temp = null;
				}
				
			}			
		});	
		
		
		
		x.join(y)
		.where(new KeySelector<Tuple5<String,String,String,String,String>,String>(){

			@Override
			public String getKey(Tuple5<String, String, String,String,String> value) throws Exception {
				return value.f2;
			}		
		})
		.equalTo(new KeySelector<Tuple3<String,String,String>,String>(){

			@Override
			public String getKey(Tuple3<String, String, String> value) throws Exception {
				return value.f0;
			}
			
		})
		.window(TumblingEventTimeWindows.of(Time.seconds(1)))
		.apply(new JoinFunction<Tuple5<String, String, String,String,String>,Tuple3<String, String, String>,Void>(){

			@Override
			public Void join(Tuple5<String, String, String,String,String> first, Tuple3<String, String, String> second)
					throws Exception {
				System.out.println(RDFStreamSource.timestampToDate(Calendar.getInstance().getTimeInMillis()) + ">>");
				System.out.println("X: "+first.f0 +" , Y: " +second.f0 +" , Z: " + first.f4);
				return null;				
			}
		}); 
		
		
	}	

	/*
	* query 9:
	* */



	/*
	 query10:
	*/

	
	
	/*
	 query11:
	 PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
	 PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
	 SELECT ?X
	 WHERE
	 {   ?X rdf:type ub:ResearchGroup .
  		 ?X ub:subOrganizationOf ?y .
  		 ?y ub:subOrganizationOf <http://www.University0.edu>
  	 }
	 */
	public static void query11(DataStream<RDFQuadruple> rdfs) {
		
		rdfs.flatMap(new FlatMapFunction<RDFQuadruple,Tuple3<String,String,String>>(){
			
			@Override
			public void flatMap(RDFQuadruple value, Collector<Tuple3<String, String, String>> out) throws Exception {					
				out.collect(new Tuple3(value.getSubject(),value.getPredicate(),value.getObject()));								
			}					
		})
		.filter(new FilterFunction<Tuple3<String,String,String>>(){
			
			@Override
			public boolean filter(Tuple3<String, String, String> value) throws Exception {
				if((value.f1.equals(RDFTerm.type) && value.f2.equals(RDFTerm.researchGroup))
                        || ((value.f1.equals(RDFTerm.subOrganOf) && value.f2.equals(RDFTerm.Univ0))))
					return true;
				else
					return false;
			}		
		})	
		.keyBy(0)
		.timeWindow(Time.seconds(1), Time.seconds(1))
		.apply(new WindowFunction<Tuple3<String,String,String>,String,Tuple,TimeWindow>(){

			@Override
			public void apply(Tuple key, TimeWindow window, Iterable<Tuple3<String, String, String>> input,
					Collector<String> out) throws Exception {
				boolean condition1 = false;
				boolean condition2 = false;
				String subject = null;
				for(Tuple3<String, String, String> in: input) {
					subject = in.f0;
					if((in.f1.equals(RDFTerm.type) && in.f2.equals(RDFTerm.researchGroup)))
						condition1 = true;
					if((in.f1.equals(RDFTerm.subOrganOf) && in.f2.equals(RDFTerm.Univ0)))
						condition2 = true;		
				}
				if(condition1 && condition2) {
					System.out.println(RDFStreamSource.timestampToDate(Calendar.getInstance().getTimeInMillis()) + ">>");
					System.out.println("X: "+subject);
					out.collect(subject);
				}
			}		
		});		
	}

	
	/*
	 query12:
	 PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
	 PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
	 SELECT ?X, ?Y
	 WHERE
	 {	?X rdf:headof ?Z .
	    ?Z rdf:type ub:Department . //?X rdf:type ub:Chair .
  	 	?Y rdf:type ub:Department .
  	 	?X ub:worksFor ?Y .
  	 	?Y ub:subOrganizationOf <http://www.University0.edu>
  	 }	
	 */	
	public static void query12(DataStream<RDFQuadruple> rdfs) {
		
		HashSet<Tuple3<String,String,String>> sets = new HashSet<Tuple3<String,String,String>>();
		
		DataStream<Tuple3<String,String,String>> tuple3 = rdfs.flatMap(new FlatMapFunction<RDFQuadruple,Tuple3<String,String,String>>(){

			public void flatMap(RDFQuadruple value, Collector<Tuple3<String, String, String>> out) throws Exception {
				
				out.collect(new Tuple3(value.getSubject(),value.getPredicate(),value.getObject()));								
			}					
		});
		
		DataStream<Tuple3<String, String, String>> y = tuple3.filter(new FilterFunction<Tuple3<String,String,String>>(){
		
			public boolean filter(Tuple3<String, String, String> value) throws Exception {
				if ((value.f1.equals(RDFTerm.type) && value.f2.equals(RDFTerm.department))
                        || (value.f1.equals(RDFTerm.subOrganOf) && value.f2.equals(RDFTerm.Univ0)) )
					return true;
				else
					return false;
			}
	    	
	    })
		.keyBy(0)
		.timeWindow(Time.seconds(5),Time.seconds(1))
		.apply(new WindowFunction<Tuple3<String,String,String>,Tuple3<String,String,String>,Tuple,TimeWindow>(){

			@Override
			public void apply(Tuple key, TimeWindow window, Iterable<Tuple3<String, String, String>> input,
					Collector<Tuple3<String, String, String>> out) throws Exception {
				
				boolean condition1 = false;
				boolean condition2 = false;
				for(Tuple3<String,String,String> in: input) {
					if(in.f1.equals(RDFTerm.type) && in.f2.equals(RDFTerm.department))
						condition1 = true;	
					if(in.f1.equals(RDFTerm.subOrganOf) && in.f2.equals(RDFTerm.Univ0))
						condition2 = true;
					if(condition1 && condition2) {
						break;
					}
				}
				if(condition1 && condition2) {
					for(Tuple3<String,String,String> in: input) {
						if(in.f1.equals(RDFTerm.subOrganOf) && in.f2.equals(RDFTerm.Univ0))    //已经满足两个条件，选择其中之一就行
							sets.add(in);
					}					
					//System.out.println(RDFStreamSource.timestampToDate(Calendar.getInstance().getTimeInMillis()) + ">>");
					Iterator i = sets.iterator();
					while(i.hasNext()) {
						Object obj = i.next();
						out.collect((Tuple3<String, String, String>) obj);
						//System.out.println(obj);
						
					}
					sets.clear();
				}
			
			}			
		});
		
		DataStream<Tuple3<String,String,String>> x = tuple3.filter(new FilterFunction<Tuple3<String,String,String>>(){
			
			public boolean filter(Tuple3<String, String, String> value) throws Exception {
				if ((value.f1.equals(RDFTerm.type) && value.f2.equals(RDFTerm.name))|| (value.f1.equals(RDFTerm.worksFor)))
					return true;
				else
					return false;
			}
	    	
	    })
        .keyBy(0)
		.timeWindow(Time.seconds(5),Time.seconds(1))
		.apply(new WindowFunction<Tuple3<String,String,String>,Tuple3<String,String,String>,Tuple,TimeWindow>(){

			@Override
			public void apply(Tuple key, TimeWindow window, Iterable<Tuple3<String, String, String>> input,
					Collector<Tuple3<String, String, String>> out) throws Exception {
				
				boolean condition1 = false;
				boolean condition2 = false;
				for(Tuple3<String,String,String> in: input) {
					if(in.f1.equals(RDFTerm.type) && in.f2.equals(RDFTerm.name))
						condition1 = true;	
					if(in.f1.equals(RDFTerm.worksFor)) {
						condition2 = true;
					}
				}
				if(condition1 && condition2) {
					for(Tuple3<String,String,String> in: input) {
						if(in.f1.equals(RDFTerm.worksFor))  //此处是predicate=13
							sets.add(in);
					}					
					//System.out.println(RDFStreamSource.timestampToDate(Calendar.getInstance().getTimeInMillis()) + ">>");
					Iterator j = sets.iterator();
					while(j.hasNext()) {
						Object objs = j.next();
						out.collect((Tuple3<String, String, String>) objs);
						//System.out.println(obj);
						
					}
					sets.clear();
				}			
			}			
		});	
		
		
		
		x.join(y)
		.where(new KeySelector<Tuple3<String,String,String>,String>(){

			@Override
			public String getKey(Tuple3<String, String, String> value) throws Exception {
				return value.f2;
			}		
		})
		.equalTo(new KeySelector<Tuple3<String,String,String>,String>(){

			@Override
			public String getKey(Tuple3<String, String, String> value) throws Exception {
				return value.f0;
			}
			
		})
		.window(TumblingEventTimeWindows.of(Time.seconds(1)))
		.apply(new JoinFunction<Tuple3<String, String, String>,Tuple3<String, String, String>,Void>(){

			@Override
			public Void join(Tuple3<String, String, String> first, Tuple3<String, String, String> second)
					throws Exception {
				System.out.println(RDFStreamSource.timestampToDate(Calendar.getInstance().getTimeInMillis()) + ">>");
				System.out.println("X: "+first.f0 +" , Y: " +second.f0);
				return null;				
			}
		}); 
		
		
	}
	
	
	
	/*query13
	 PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
	 PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
	 SELECT ?X
	 WHERE
	 {?X rdf:type ub:Person .
  	  <http://www.University0.edu> ub:hasAlumnus ?X}	 
	 */
	public static void query13(DataStream<RDFQuadruple> rdfs) {
		
		
		DataStream<Tuple3<String,String,String>> tuple3 = rdfs.flatMap(new FlatMapFunction<RDFQuadruple,Tuple3<String,String,String>>(){

			public void flatMap(RDFQuadruple value, Collector<Tuple3<String, String, String>> out) throws Exception {					
				out.collect(new Tuple3(value.getSubject(),value.getPredicate(),value.getObject()));								
			}					
		});
	    
		DataStream<Tuple3<String,String,String>> stream1 = tuple3.filter(new FilterFunction<Tuple3<String,String,String>>(){
		
			public boolean filter(Tuple3<String, String, String> value) throws Exception {
				if (value.f0.equals(RDFTerm.Univ0) &&
                        (value.f1.equals(RDFTerm.undergradDegreeFrom)
                                || value.f1.equals(RDFTerm.masterDegreeFrom) || value.f1.equals(RDFTerm.doctoralDegreeFrom) ))
					return true;
				else
					return false;
			}
	    	
	    });
		
		DataStream<Tuple3<String,String,String>> stream2 = tuple3.filter(new FilterFunction<Tuple3<String,String,String>>(){
			
			public boolean filter(Tuple3<String, String, String> value) throws Exception {
				if (value.f1.equals(RDFTerm.type) &&
                        (value.f2.equals(RDFTerm.gradStu)
                        || value.f2.equals(RDFTerm.undergradStu) ))
					return true;
				else
					return false;
			}
	    	
	    });
		
		stream1.join(stream2)
		.where(new KeySelector<Tuple3<String,String,String>,String>(){

			@Override
			public String getKey(Tuple3<String, String, String> value) throws Exception {
				return value.f2;
			}	
		})
		.equalTo(new KeySelector<Tuple3<String,String,String>,String>(){

			@Override
			public String getKey(Tuple3<String, String, String> value) throws Exception {
				return value.f0;
			}		
		})
		.window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1)))
		.apply(new JoinFunction<Tuple3<String, String, String>,Tuple3<String, String, String>,Void>(){

			@Override
			public Void join(Tuple3<String, String, String> first, Tuple3<String, String, String> second)
					throws Exception {
				System.out.println(RDFStreamSource.timestampToDate(Calendar.getInstance().getTimeInMillis()) + ">>");
				System.out.println("X: "+first.f2);
				return null;				
			}

		});
		
	}
	
	
	
	
	/*
	 query14-
	 PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
	 PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
	 SELECT ?X
	 WHERE {?X rdf:type ub:UndergraduateStudent}
	 */
	public static void query14(DataStream<RDFQuadruple> rdfs) {
		rdfs.flatMap(new FlatMapFunction<RDFQuadruple,Tuple3<String,String,String>>(){

			@Override
			public void flatMap(RDFQuadruple value, Collector<Tuple3<String, String, String>> out) throws Exception {
				out.collect(new Tuple3(value.getSubject(),value.getPredicate(),value.getObject()));					
			}
			
		})
		.filter(new FilterFunction<Tuple3<String,String,String>>(){

			@Override
			public boolean filter(Tuple3<String, String, String> value) throws Exception {
				if(value.f1.equals(RDFTerm.type) && value.f2.equals(RDFTerm.undergradStu))
					return true;
				else
					return false;
			}			
		})
		.keyBy(0)
		.timeWindow(Time.seconds(5), Time.seconds(1))
		.apply(new WindowFunction<Tuple3<String,String,String>,String,Tuple,TimeWindow>(){

			@Override
			public void apply(Tuple key, TimeWindow window, Iterable<Tuple3<String, String, String>> input,
					Collector<String> out) throws Exception {
				System.out.println(RDFStreamSource.timestampToDate(Calendar.getInstance().getTimeInMillis()) + ">>");
				for(Tuple3<String, String, String> in: input) {
					System.out.println("X: "+in.f0);
					out.collect(in.f0);
					break;
				}			
			}			
		});
		
	}
	
	
}
