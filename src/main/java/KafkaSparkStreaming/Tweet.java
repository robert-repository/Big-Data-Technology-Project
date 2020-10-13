package KafkaSparkStreaming;

import com.google.gson.JsonObject;
import com.google.gson.annotations.SerializedName;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;


public class Tweet {

	private long id;
	private String text;
	private String lang;
	private User user;
	private String source;

	private Place place;
	
	@SerializedName("created_at")
	private String createdAt;
	
	
	@SerializedName("retweet_count")
	private int retweetCount;
	
	@SerializedName("favorite_count")
	private int favoriteCount;

	public long getId() {
		// TODO Auto-generated method stub
		return id;
	}
	
	

	public String getText() {
		return text;
	}



	public void setText(String text) {
		this.text = text;
	}



	public String getLang() {
		return lang;
	}



	public void setLang(String lang) {
		this.lang = lang;
	}



	public User getUser() {
		return user;
	}



	public void setUser(User user) {
		this.user = user;
	}



	public String getSource() {
		return source;
	}



	public void setSource(String source) {
		this.source = source;
	}



	public Place getPlace() {
		return place;
	}



	public void setPlace(Place place) {
		this.place = place;
	}



	public String getCreatedAt() throws ParseException {
		String[] t = createdAt.split(" ");
		Date date = new SimpleDateFormat("MMM").parse(t[1]);
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		
		String d =  cal.get(Calendar.MONTH)+1+"";
		System.out.println("month "+ d);
		/*d += " " +  t[3];
		SimpleDateFormat format = new SimpleDateFormat("MMM dd yyyy HH:mm:ss");*/
		return String.join("-", t[5], d, t[2]);
	}



	public void setCreatedAt(String createdAt) {
		this.createdAt = createdAt;
	}



	public int getRetweetCount() {
		return retweetCount;
	}



	public void setRetweetCount(int retweetCount) {
		this.retweetCount = retweetCount;
	}



	public int getFavoriteCount() {
		return favoriteCount;
	}



	public void setFavoriteCount(int favoriteCount) {
		this.favoriteCount = favoriteCount;
	}



	public void setId(long id) {
		this.id = id;
	}



	@Override
	public String toString() {
		JsonObject obj = new JsonObject();
		obj.addProperty("created_at", createdAt);
		obj.addProperty("tweet_id", id);	
		obj.addProperty("user", user.toString());	
		obj.addProperty("favorit_count", favoriteCount);
		obj.addProperty("text", text);
		obj.addProperty("lang", lang);
		obj.addProperty("source", source);
		obj.addProperty("retweet_count", retweetCount);
		obj.addProperty("place", place == null ? "" :place.toString());
		
		return obj.toString();
		
	}
	
	
	
}
