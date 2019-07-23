package com.pojo;

import java.io.Serializable;


public class ClassBrands implements Serializable {
	
	private static final long serialVersionUID = 1L;
	public String Country;
	public String Division;
	public String BU;
	public String Studio;
	public String Neighborhoods;
	public String Brand_Chapter;
	public String Beneficiary;
	public String Catlib;
	public String ProdKey;
	public String getCountry() {
		return Country;
	}
	public void setCountry(String country) {
		Country = country;
	}
	public String getDivision() {
		return Division;
	}
	public void setDivision(String division) {
		Division = division;
	}
	public String getBU() {
		return BU;
	}
	public void setBU(String bU) {
		BU = bU;
	}
	public String getStudio() {
		return Studio;
	}
	public void setStudio(String studio) {
		Studio = studio;
	}
	public String getNeighborhoods() {
		return Neighborhoods;
	}
	public void setNeighborhoods(String neighborhoods) {
		Neighborhoods = neighborhoods;
	}
	public String getBrand_Chapter() {
		return Brand_Chapter;
	}
	public void setBrand_Chapter(String brand_Chapter) {
		Brand_Chapter = brand_Chapter;
	}
	public String getBeneficiary() {
		return Beneficiary;
	}
	public void setBeneficiary(String beneficiary) {
		Beneficiary = beneficiary;
	}
	public String getCatlib() {
		return Catlib;
	}
	public void setCatlib(String catlib) {
		Catlib = catlib;
	}
	public String getProdKey() {
		return ProdKey;
	}
	public void setProdKey(String prodKey) {
		ProdKey = prodKey;
	}
	
	
}
