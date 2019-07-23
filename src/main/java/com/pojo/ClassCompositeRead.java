package com.pojo;

import java.io.Serializable;

public class ClassCompositeRead implements Serializable {

	private static final long serialVersionUID = 1L;
	public String compositekey;
	public String subchannel;
	
	
	public String getcompositekey() {
		return compositekey;
	}
	public void setcompositekey(String compositeKey) {
		compositekey = compositeKey;
	}
	public String getsubchannel() {
		return subchannel;
	}
	public void setsubchannel(String subChannel) {
		subchannel = subChannel;
	}
	
}
