package com.pojo;
import java.io.Serializable;
public class ClassEvents implements Serializable{
	private static final long serialVersionUID = 1L;
	public String EventType;
	public String EventKey;
	public String EventName;
	public String EventComponents;
	public String getEventType() {
		return EventType;
	}
	public void setEventType(String eventType) {
		EventType = eventType;
	}
	public String getEventKey() {
		return EventKey;
	}
	public void setEventKey(String eventKey) {
		EventKey = eventKey;
	}
	public String getEventName() {
		return EventName;
	}
	public void setEventName(String eventName) {
		EventName = eventName;
	}
	public String getEventComponents() {
		return EventComponents;
	}
	public void setEventComponents(String eventComponents) {
		EventComponents = eventComponents;
	}
	
}
