package com.pojo;

import java.io.Serializable;

public class Durationflag implements Serializable{
	private static final long serialVersionUID = 1L;
	public String Outlet;
	public String Weekint;
	public String Limit1;
	public String eventidA;
	public String Dummy;
	public String mindate;
	public String maxdate;
	public String Weekend;
	public String EveDate;
	public Double grp;
	public Double flag;
	public String getOutlet() {
		return Outlet;
	}
	public void setOutlet(String outlet) {
		Outlet = outlet;
	}
	public String getWeekint() {
		return Weekint;
	}
	public void setWeekint(String weekint) {
		Weekint = weekint;
	}
	public String getLimit1() {
		return Limit1;
	}
	public void setLimit1(String limit1) {
		Limit1 = limit1;
	}
	public String getEventidA() {
		return eventidA;
	}
	public void setEventidA(String eventidA) {
		this.eventidA = eventidA;
	}
	public String getDummy() {
		return Dummy;
	}
	public void setDummy(String dummy) {
		Dummy = dummy;
	}
	public String getMindate() {
		return mindate;
	}
	public void setMindate(String mindate) {
		this.mindate = mindate;
	}
	public String getMaxdate() {
		return maxdate;
	}
	public void setMaxdate(String maxdate) {
		this.maxdate = maxdate;
	}
	public String getWeekend() {
		return Weekend;
	}
	public void setWeekend(String weekend) {
		Weekend = weekend;
	}
	public String getEveDate() {
		return EveDate;
	}
	public void setEveDate(String eveDate) {
		EveDate = eveDate;
	}
	public Double getGrp() {
		return grp;
	}
	public void setGrp(Double grp) {
		this.grp = grp;
	}
	public Double getFlag() {
		return flag;
	}
	public void setFlag(Double flag) {
		this.flag = flag;
	}
	public static long getSerialversionuid() {
		return serialVersionUID;
	}
	
}
