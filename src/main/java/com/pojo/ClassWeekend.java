package com.pojo;
import java.io.Serializable;
import java.util.Date;

public class ClassWeekend implements Serializable {

	private static final long serialVersionUID = 1L;
	public Date EveDate;

	public Date getEveDate() {
		return EveDate;
	}

	public void setEveDate(Date eveDate) {
		EveDate = eveDate;
	}
	
}
