package com.yieldbook.mortgage.hbase.bean;

public class LoanFhlmc {
	private String loan_identifier;
	private String loan_correction_indicator;
	private String prefix;
	private String security_identifier;
	private String cusip;
	private double mortgage_loan_amount;
	private double issuance_investor_loan_upb;
	private double current_investor_loan_upb;
	private String amortization_type;
	private float original_interest_rate;
	public String getLoan_identifier() {
		return loan_identifier;
	}
	public void setLoan_identifier(String loan_identifier) {
		this.loan_identifier = loan_identifier;
	}
	public String getLoan_correction_indicator() {
		return loan_correction_indicator;
	}
	public void setLoan_correction_indicator(String loan_correction_indicator) {
		this.loan_correction_indicator = loan_correction_indicator;
	}
	public String getPrefix() {
		return prefix;
	}
	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}
	public String getSecurity_identifier() {
		return security_identifier;
	}
	public void setSecurity_identifier(String security_identifier) {
		this.security_identifier = security_identifier;
	}
	public String getCusip() {
		return cusip;
	}
	public void setCusip(String cusip) {
		this.cusip = cusip;
	}
	public double getMortgage_loan_amount() {
		return mortgage_loan_amount;
	}
	public void setMortgage_loan_amount(double mortgage_loan_amount) {
		this.mortgage_loan_amount = mortgage_loan_amount;
	}
	public double getIssuance_investor_loan_upb() {
		return issuance_investor_loan_upb;
	}
	public void setIssuance_investor_loan_upb(double issuance_investor_loan_upb) {
		this.issuance_investor_loan_upb = issuance_investor_loan_upb;
	}
	public double getCurrent_investor_loan_upb() {
		return current_investor_loan_upb;
	}
	public void setCurrent_investor_loan_upb(double current_investor_loan_upb) {
		this.current_investor_loan_upb = current_investor_loan_upb;
	}
	public String getAmortization_type() {
		return amortization_type;
	}
	public void setAmortization_type(String amortization_type) {
		this.amortization_type = amortization_type;
	}
	public float getOriginal_interest_rate() {
		return original_interest_rate;
	}
	public void setOriginal_interest_rate(float original_interest_rate) {
		this.original_interest_rate = original_interest_rate;
	}
	@Override
	public String toString() {
		return "LoanFhlmc [loan_identifier=" + loan_identifier
				+ ", loan_correction_indicator=" + loan_correction_indicator
				+ ", prefix=" + prefix + ", security_identifier="
				+ security_identifier + ", cusip=" + cusip
				+ ", mortgage_loan_amount=" + mortgage_loan_amount
				+ ", issuance_investor_loan_upb=" + issuance_investor_loan_upb
				+ ", current_investor_loan_upb=" + current_investor_loan_upb
				+ ", amortization_type=" + amortization_type
				+ ", original_interest_rate=" + original_interest_rate + "]";
	}
	
	
}
