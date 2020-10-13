package KafkaSparkStreaming;

import com.google.gson.JsonObject;
import com.google.gson.annotations.SerializedName;

public class User {

	private long id;
	private String name;
	
	@SerializedName("screen_name")
	private String screenName;
	
	private String location;
	
	@SerializedName("followers_count")
	private int followersCount;

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getScreenName() {
		return screenName;
	}

	public void setScreenName(String screenName) {
		this.screenName = screenName;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public int getFollowersCount() {
		return followersCount;
	}

	public void setFollowersCount(int followersCount) {
		this.followersCount = followersCount;
	}
	
	@Override
	public String toString() {
		JsonObject obj = new JsonObject();
		obj.addProperty("id", id);
		obj.addProperty("name", name);	
		obj.addProperty("screen_name", screenName);	
		obj.addProperty("location", location);
		obj.addProperty("followers_count", followersCount);
		
		return obj.toString();
		
	}
}
