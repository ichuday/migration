package com.pojo;

import java.io.Serializable;

public class ClassCurves implements Serializable {

	private static final long serialVersionUID = 1L;
	public String Brand;
	public String SBU;
	public String Division;
	public double Alpha;
	public double Beta;
	public String Market;
	public String SubChannel;
	public String getBrand() {
		return Brand;
	}
	public void setBrand(String brand) {
		Brand = brand;
	}
	public String getSBU() {
		return SBU;
	}
	public void setSBU(String sBU) {
		SBU = sBU;
	}
	public String getDivision() {
		return Division;
	}
	public void setDivision(String division) {
		Division = division;
	}
	public double getAlpha() {
		return Alpha;
	}
	public void setAlpha(double alpha) {
		Alpha = alpha;
	}
	public double getBeta() {
		return Beta;
	}
	public void setBeta(double beta) {
		Beta = beta;
	}
	public String getMarket() {
		return Market;
	}
	public void setMarket(String market) {
		Market = market;
	}
	public String getSubChannel() {
		return SubChannel;
	}
	public void setSubChannel(String subChannel) {
		SubChannel = subChannel;
	}
	
}
