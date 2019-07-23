package com.pojo;

import java.io.Serializable;

public class ClassBasis implements Serializable {

	private static final long serialVersionUID = 1L;
	public String SourceBDA;
	public String Brand;
	public String BasisYearPY;
	public String BasisYearP2Y;
	public String BasisYearP3Y;
	
	public String getSourceBDA() {
		return SourceBDA;
	}
	public void setSourceBDA(String sourceBDA) {
		SourceBDA = sourceBDA;
	}
	public String getBrand() {
		return Brand;
	}
	public void setBrand(String brand) {
		Brand = brand;
	}
	public String getBasisYearPY() {
		return BasisYearPY;
	}
	public void setBasisYearPY(String basisyearPY) {
		BasisYearPY = basisyearPY;
	}
	
	public String getBasisYearP2Y() {
		return BasisYearP2Y;
	}
	public void setBasisYearP2Y(String basisyearP2Y) {
		BasisYearP2Y = basisyearP2Y;
	}
	
	public String getBasisYearP3Y() {
		return BasisYearP3Y;
	}
	public void setBasisYearP3Y(String basisyearP3Y) {
		BasisYearP3Y = basisyearP3Y;
	}
	
}
