package KafkaSparkStreaming;

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


public class App 
{
	 public static JavaSparkContext sparkContext;
	 private static final String TABLE_NAME = "tweetinfos";
	 private static final String CF1 = "tweet";
	 private static final String CF11 = "post_id";
	 private static final String CF12 = "text";
	 private static final String CF13 = "lang";
	 private static final String CF14 = "source";
	 private static final String CF15 = "created_at";
	 private static final String CF16 = "retweet_count";
	 private static final String CF17 = "favorite_count";

	 
	 private static final String CF2 = "user";
	 private static final String CF21 = "user_id";
	 private static final String CF22 = "user_name";
	 private static final String CF23 = "screen_name";
	 private static final String CF24 = "location";
	 private static final String CF25 = "followers_count";
	 
	 private static final String CF3 = "place";
	 private static final String CF31 = "place_type";
	 private static final String CF32 = "name";
	 private static final String CF33 = "full_name";
	 private static final String CF34 = "country";
	 

	 
	 static Configuration config;
	 static Connection connection;
	 static Table table;
	 
    public static void main( String[] args )
    {
    	init();
    	
    	Map<String, Object> kafkaParams = new HashMap<>();
    	kafkaParams.put("bootstrap.servers", "localhost:9092");
    	kafkaParams.put("key.deserializer", StringDeserializer.class);
    	kafkaParams.put("value.deserializer", StringDeserializer.class);
    	kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
    	kafkaParams.put("auto.offset.reset", "latest");
    	kafkaParams.put("enable.auto.commit", false);

    	Collection<String> topics = Arrays.asList("health-tweets", "bidata-tweets", "food-tweets", "love-tweets");
    	
    	SparkConf sparkConf = new SparkConf();
    	sparkConf.setAppName("KafkaStreaming");
    	sparkConf.setMaster("local[*]");
    	//sparkConf.set("spark.cassandra.connection.host", "127.0.0.1");
    	 
    	JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(3));
    	//sparkContext = streamingContext.sparkContext();
    	System.out.println("context: ");
    	
    	//Configuration config = HBaseConfiguration.create();


    	JavaInputDStream<ConsumerRecord<String, String>> messages =
    			  KafkaUtils.createDirectStream(
    			    streamingContext, 
    			    LocationStrategies.PreferConsistent(), 
    			    ConsumerStrategies.<String, String> Subscribe(topics, kafkaParams));
    	System.out.println("retreiving data: ");
    	JavaPairDStream<String, String> results = messages
    			  .mapToPair( 
    			      record ->{
    			    	  System.out.println("record: "+ record.key());
    			    	  System.out.println("record: "+ record.value());
    			    	  return new Tuple2<>(record.key(), record.value());
    			      }
    			      
    			  );
    			JavaDStream<String> lines = results
    			  .map(
    			      tuple2 ->{
    			    	  return tuple2._2();
    			    	  
    			      }
    			  );
    			
    			//JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split("\\s+")).iterator());
    			JavaPairDStream<String, Integer> wordCounts = lines
    			  .mapToPair(
    			      s -> new Tuple2<>(s, 1)
    			  ).reduceByKey(
    			      (i1, i2) -> i1 + i2
    			    );
    
    				wordCounts.foreachRDD(
        				    javaRdd -> {
        				      Map<String, Integer> wordCountMap = javaRdd.collectAsMap();
        				      for (String key : wordCountMap.keySet()) {
        				    	  Gson g = new Gson();
        				    	  Tweet tweet = g.fromJson(key, Tweet.class);
        				    	  User user = tweet.getUser();
        				    	  Place place = tweet.getPlace();
        				    	  System.out.println("tweet Lang:"+ tweet.getLang());
        				  
        				    	  
        				    	  Put p = new Put(Bytes.toBytes(tweet.getId()+""));
        				    	  p.addImmutable(CF1.getBytes(), Bytes.toBytes(CF11),Bytes.toBytes(tweet.getId()+""));
        				    	  p.addImmutable(CF1.getBytes(), Bytes.toBytes(CF12),Bytes.toBytes(tweet.getText()));
        						  p.addImmutable(CF1.getBytes(), Bytes.toBytes(CF13),Bytes.toBytes(tweet.getLang()));
        						  p.addImmutable(CF1.getBytes(), Bytes.toBytes(CF14),Bytes.toBytes(tweet.getSource()));
        						  p.addImmutable(CF1.getBytes(), Bytes.toBytes(CF15),Bytes.toBytes(tweet.getCreatedAt()));
        						  p.addImmutable(CF1.getBytes(), Bytes.toBytes(CF16),Bytes.toBytes(tweet.getRetweetCount()+""));
        						  p.addImmutable(CF1.getBytes(), Bytes.toBytes(CF17),Bytes.toBytes(tweet.getFavoriteCount()+"" ));
        						  
        						  p.addImmutable(CF2.getBytes(), Bytes.toBytes(CF21),Bytes.toBytes(user.getId()+""));
        				    	  p.addImmutable(CF2.getBytes(), Bytes.toBytes(CF22),Bytes.toBytes(user.getName()));
        						  p.addImmutable(CF2.getBytes(), Bytes.toBytes(CF23),Bytes.toBytes(user.getScreenName() == null ? "":user.getScreenName()));
        						  p.addImmutable(CF2.getBytes(), Bytes.toBytes(CF24),Bytes.toBytes(user.getLocation()== null ? "": user.getLocation()));
        						  p.addImmutable(CF2.getBytes(), Bytes.toBytes(CF25),Bytes.toBytes(user.getFollowersCount()+""));
        						  
        						  p.addImmutable(CF3.getBytes(), Bytes.toBytes(CF31),Bytes.toBytes(place == null ? "":(place.getPlaceType() == null ? "":place.getPlaceType())));
        						  p.addImmutable(CF3.getBytes(), Bytes.toBytes(CF32),Bytes.toBytes(place == null ? "":(place.getName() == null ? "":place.getName()) ));
        						  p.addImmutable(CF3.getBytes(), Bytes.toBytes(CF33),Bytes.toBytes(place == null ? "":(place.getFullName() == null ? "":place.getFullName()) ));
        						  p.addImmutable(CF3.getBytes(), Bytes.toBytes(CF34),Bytes.toBytes(place == null ? "":(place.getCountry() == null ? "":place.getCountry())));
        						  
        							table.put(p);
        				    	  System.out.println("insertion post:"+ tweet.getId());
        				      }
        				    }
        				  );
    		
    				System.out.println("insert success!");
    				
    				
    			//} catch (IOException e1) {
					// TODO Auto-generated catch block
				//	e1.printStackTrace();
				//}
    			
    			System.out.println("Check");
    			streamingContext.start();
    			try {
					streamingContext.awaitTermination();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    }
    
    public static void init(){
    	 config = HBaseConfiguration.create();
    	 try {
			connection = ConnectionFactory.createConnection(config);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	 try {
			table = connection.getTable(TableName.valueOf(TABLE_NAME));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}
