package com.pojo;

import java.io.Serializable;


public class ClassFinance implements Serializable {

	private static final long serialVersionUID = 1L;
	public String BeneficiaryFinance;
	public String CatlibCode;
	public double rNR;
	public double rNCS;
	public double rCtb;
	public double rAC;
	public String getBeneficiaryFinance() {
		return BeneficiaryFinance;
	}
	public void setBeneficiaryFinance(String beneficiaryFinance) {
		BeneficiaryFinance = beneficiaryFinance;
	}
	public String getCatlibCode() {
		return CatlibCode;
	}
	public void setCatlibCode(String catlibCode) {
		CatlibCode = catlibCode;
	}
	public double getrNR() {
		return rNR;
	}
	public void setrNR(double rNR) {
		this.rNR = rNR;
	}
	public double getrNCS() {
		return rNCS;
	}
	public void setrNCS(double rNCS) {
		this.rNCS = rNCS;
	}
	public double getrCtb() {
		return rCtb;
	}
	public void setrCtb(double rCtb) {
		this.rCtb = rCtb;
	}
	public double getrAC() {
		return rAC;
	}
	public void setrAC(double rAC) {
		this.rAC = rAC;
	}
	
	
}
