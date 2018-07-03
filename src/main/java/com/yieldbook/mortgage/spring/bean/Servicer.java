package com.yieldbook.mortgage.spring.bean;

import java.sql.Timestamp;

public class Servicer {
	private int ServicerId;
	
	private String Servicer;
	
	public Servicer(){}
	
	public Servicer(int servicerId, String servicer, Timestamp lastChgDate) {
		ServicerId = servicerId;
		Servicer = servicer;
		LastChgDate = lastChgDate;
	}

	public Servicer(String servicer, Timestamp lastChgDate) {
		Servicer = servicer;
		LastChgDate = lastChgDate;
	}
	
	public Servicer(int servicerId) {
		super();
		ServicerId = servicerId;
	}

	private Timestamp LastChgDate;

	public int getServicerId() {
		return ServicerId;
	}

	public void setServicerId(int servicerId) {
		ServicerId = servicerId;
	}

	public String getServicer() {
		return Servicer;
	}

	public void setServicer(String servicer) {
		Servicer = servicer;
	}

	public Timestamp getLastChgDate() {
		return LastChgDate;
	}

	public void setLastChgDate(Timestamp lastChgDate) {
		LastChgDate = lastChgDate;
	}

	@Override
	public String toString() {
		return "Servicer [ServicerId=" + ServicerId + ", Servicer=" + Servicer
				+ ", LastChgDate=" + LastChgDate + "]";
	}

}
