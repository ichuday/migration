package com.pojo;

import java.io.Serializable;

public class ClassEventExec implements Serializable{

	private static final long serialVersionUID = 1L;
	public String Brand;
	public String Copy;
	public String Start;
	public String Stop;
	public double GRPs;
	public String EvntType;
	public String EvntKey;
	public String EvntName;
	public String getBrand() {
		return Brand;
	}
	public void setBrand(String brand) {
		Brand = brand;
	}
	public String getCopy() {
		return Copy;
	}
	public void setCopy(String copy) {
		Copy = copy;
	}
	public String getStart() {
		return Start;
	}
	public void setStart(String start) {
		Start = start;
	}
	public String getStop() {
		return Stop;
	}
	public void setStop(String stop) {
		Stop = stop;
	}
	public double getGRPs() {
		return GRPs;
	}
	public void setGRPs(double gRPs) {
		GRPs = gRPs;
	}
	public String getEvntType() {
		return EvntType;
	}
	public void setEvntType(String evntType) {
		EvntType = evntType;
	}
	public String getEvntKey() {
		return EvntKey;
	}
	public void setEvntKey(String evntKey) {
		EvntKey = evntKey;
	}
	public String getEvntName() {
		return EvntName;
	}
	public void setEvntName(String evntName) {
		EvntName = evntName;
	}
	

}
