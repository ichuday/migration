package com.pojo;

import java.io.Serializable;
import java.util.Date;

public class ClassHispanic implements Serializable{

	private static final long serialVersionUID = 1L;
	public String Market;
	public Date WeekEnding;
	public String BrandVariant;
	public String Creative;
	public String Brand;
	public String CommercialDuration;
	public double Advertisement_id;
	public double TVHousehold;
	
	public String getMarket() {
		return Market;
	}
	public void setMarket(String market) {
		Market = market;
	}
	public Date getWeekEnding() {
		return WeekEnding;
	}
	public void setWeekEnding(Date weekEnding) {
		WeekEnding = weekEnding;
	}
	public String getBrandVariant() {
		return BrandVariant;
	}
	public void setBrandVariant(String brandVariant) {
		BrandVariant = brandVariant;
	}
	public String getCreative() {
		return Creative;
	}
	public void setCreative(String creative) {
		Creative = creative;
	}
	public String getBrand() {
		return Brand;
	}
	public void setBrand(String brand) {
		Brand = brand;
	}
	public String getCommercialDuration() {
		return CommercialDuration;
	}
	public void setCommercialDuration(String commercialDuration) {
		CommercialDuration = commercialDuration;
	}
	public double getAdvertisement_id() {
		return Advertisement_id;
	}
	public void setAdvertisement_id(double advertisement_id) {
		Advertisement_id = advertisement_id;
	}
	public double getTVHousehold() {
		return TVHousehold;
	}
	public void setTVHousehold(double tVHousehold) {
		TVHousehold = tVHousehold;
	}
	

}


