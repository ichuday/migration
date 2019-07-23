package com.pojo;

import java.io.Serializable;

public class ClassEvent1 implements Serializable{

	private static final long serialVersionUID = 1L;
	public String EvntType;
	public String EvntKey;
	public String EvntName;
	public String EvntComponents;
	public String CustEvntStrt;
	public String CustEvntEnd;
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
	public String getEvntComponents() {
		return EvntComponents;
	}
	public void setEvntComponents(String evntComponents) {
		EvntComponents = evntComponents;
	}
	public String getCustEvntStrt() {
		return CustEvntStrt;
	}
	public void setCustEvntStrt(String custEvntStrt) {
		CustEvntStrt = custEvntStrt;
	}
	public String getCustEvntEnd() {
		return CustEvntEnd;
	}
	public void setCustEvntEnd(String custEvntEnd) {
		CustEvntEnd = custEvntEnd;
	}
	
}
