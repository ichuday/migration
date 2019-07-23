package com.pojo;
import java.io.Serializable;
import java.util.Date;

public class ClassPeriod implements Serializable{

	private static final long serialVersionUID = 1L;
	public String Source_BDA;
	public Date Start_date;
	public Date End_date;
	public String Actual_period;
	public String getSource_BDA() {
		return Source_BDA;
	}
	public void setSource_BDA(String source_BDA) {
		Source_BDA = source_BDA;
	}
	public Date getStart_date() {
		return Start_date;
	}
	public void setStart_date(Date start_date) {
		Start_date = start_date;
	}
	public Date getEnd_date() {
		return End_date;
	}
	public void setEnd_date(Date end_date) {
		End_date = end_date;
	}
	public String getActual_period() {
		return Actual_period;
	}
	public void setActual_period(String actual_period) {
		Actual_period = actual_period;
	}
	
	
	

}
