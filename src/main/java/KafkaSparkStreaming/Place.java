package KafkaSparkStreaming;

import com.google.gson.JsonObject;
import com.google.gson.annotations.SerializedName;

public class Place {
	
	@SerializedName("|place_type")
	private String placeType;
	
	private String name;
	
	@SerializedName("|full_name")
    private String fullName;
	
    private String country;
    
	public String getPlaceType() {
		return placeType;
	}

	public void setPlaceType(String placeType) {
		this.placeType = placeType;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getFullName() {
		return fullName;
	}

	public void setFullName(String fullName) {
		this.fullName = fullName;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	@Override
	public String toString() {
		JsonObject obj = new JsonObject();
		obj.addProperty("name", name);
		obj.addProperty("full_name", fullName);	
		obj.addProperty("place_type", placeType);	
		obj.addProperty("country", country);
		
		return obj.toString();
		
	}
   
}
