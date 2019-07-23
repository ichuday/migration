package com.pojo;

import java.io.Serializable;

public class ClassSpend implements Serializable {

	private static final long serialVersionUID = 1L;
	public String FiscalYear;
	public String FiscalQuarter;
	public String MTBrand;

	public String BrandChapter;
	public String Market;
	public String MarketTool;
	public String ConsumerBehavior;
	public String Channel;
	public String SubChannel;
	public String Campaign;
	public String EventName;
	public String EventKey;
	public double ReportedSpend;
	public double ModeledSpend;

	public String getFiscalYear() {
		return FiscalYear;
	}

	public void setFiscalYear(String fiscalYear) {
		FiscalYear = fiscalYear;
	}

	public String getFiscalQuarter() {
		return FiscalQuarter;
	}

	public void setFiscalQuarter(String fiscalQuarter) {
		FiscalQuarter = fiscalQuarter;
	}

	public String getBrandChapter() {
		return BrandChapter;
	}

	public void setBrandChapter(String brandChapter) {
		BrandChapter = brandChapter;
	}

	public String getMarket() {
		return Market;
	}

	public void setMarket(String market) {
		Market = market;
	}

	public String getConsumerBehavior() {
		return ConsumerBehavior;
	}

	public void setConsumerBehavior(String consumerBehavior) {
		ConsumerBehavior = consumerBehavior;
	}

	public String getChannel() {
		return Channel;
	}

	public void setChannel(String channel) {
		Channel = channel;
	}

	public String getSubChannel() {
		return SubChannel;
	}

	public void setSubChannel(String subChannel) {
		SubChannel = subChannel;
	}

	public String getCampaign() {
		return Campaign;
	}

	public void setCampaign(String campaign) {
		Campaign = campaign;
	}

	public String getEventName() {
		return EventName;
	}

	public void setEventName(String eventName) {
		EventName = eventName;
	}

	public String getEventKey() {
		return EventKey;
	}

	public void setEventKey(String eventKey) {
		EventKey = eventKey;
	}

	public double getReportedSpend() {
		return ReportedSpend;
	}

	public void setReportedSpend(double reportedSpend) {
		ReportedSpend = reportedSpend;
	}

	public double getModeledSpend() {
		return ModeledSpend;
	}

	public void setModeledSpend(double modeledSpend) {
		ModeledSpend = modeledSpend;
	}

	public String getMarketTool() {
		return MarketTool;
	}

	public void setMarketTool(String marketTool) {
		MarketTool = marketTool;
	}
	public String getMTBrand() {
		return MTBrand;
	}

	public void setMTBrand(String mTBrand) {
		MTBrand = mTBrand;
	}


}
