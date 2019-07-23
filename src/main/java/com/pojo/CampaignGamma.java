package com.pojo;
import java.io.Serializable;

public class CampaignGamma implements Serializable{
	
	private static final long serialVersionUID = 1L;
	
	public String SourceBDA;
	public String Country;
	public String Market;
	public String SubChannel;
	public String MediaChannel;
	public String ConsumerBehavior;
	public String BrandChapter;
	public String Beneficiary;
	public String Channel;
	public String Actual_Period;
	public String PeriodStartDate; //have to convert it into date
	public String PeriodEndDate;//have to convert it into date
	public double GRPs;
	public double Duration;
	public double Continuity;
	public double ReportedSpend;
	public String Level;
	public double ModeledSpend;
	public double DuetoVolume;
	public double ProjectionFactor;
	public double Volume;
	public double Alpha;
	public double Beta;
	public double rNR;
	public double rNCS;
	public double rCtb;
	public double rAC;
	public String EventName;
	public String Studio;
	public String Neighborhoods;
	public String BU;
	public String Division;
	public double BASIS_PY;
	public double BASIS_P2Y;
	public double BASIS_P3Y;
	public double BASIS_Duration_PY;
	public double BASIS_Duration_P2Y;
	public double BASIS_Duration_P3Y;
	public double Typical;
	public double Cont;
	public double AC;
	public double X1;
	public double X1_2;
	public double X1_3;
	public double X2;
	public String PeriodType;
	public String Report_Period;
	public String CatLib;
	public double Gamma_X1;
	public double Gamma_X1_2;
	public double Gamma_X1_3;
	public double Gamma_X2;
	public double ChannelVolume;
	
	public double getChannelVolume() {
		return ChannelVolume;
	}
	public void setChannelVolume(double channelVolume) {
		ChannelVolume = channelVolume;
	}
	public double getAllOutletVolume() {
		return AllOutletVolume;
	}
	public void setAllOutletVolume(double allOutletVolume) {
		AllOutletVolume = allOutletVolume;
	}
	public double AllOutletVolume;
	public String getSourceBDA() {
		return SourceBDA;
	}
	public void setSourceBDA(String sourceBDA) {
		SourceBDA = sourceBDA;
	}
	public String getCountry() {
		return Country;
	}
	public void setCountry(String country) {
		Country = country;
	}
	public String getMarket() {
		return Market;
	}
	public void setMarket(String market) {
		Market = market;
	}
	public String getSubChannel() {
		return SubChannel;
	}
	public void setSubChannel(String subChannel) {
		SubChannel = subChannel;
	}
	public String getMediaChannel() {
		return MediaChannel;
	}
	public void setMediaChannel(String mediaChannel) {
		MediaChannel = mediaChannel;
	}
	public String getConsumerBehavior() {
		return ConsumerBehavior;
	}
	public void setConsumerBehavior(String consumerBehavior) {
		ConsumerBehavior = consumerBehavior;
	}
	public String getBrandChapter() {
		return BrandChapter;
	}
	public void setBrandChapter(String brandChapter) {
		BrandChapter = brandChapter;
	}
	public String getBeneficiary() {
		return Beneficiary;
	}
	public void setBeneficiary(String beneficiary) {
		Beneficiary = beneficiary;
	}
	public String getChannel() {
		return Channel;
	}
	public void setChannel(String channel) {
		Channel = channel;
	}
	public String getActual_Period() {
		return Actual_Period;
	}
	public void setActual_Period(String actual_Period) {
		Actual_Period = actual_Period;
	}
	public String getPeriodStartDate() {
		return PeriodStartDate;
	}
	public void setPeriodStartDate(String periodStartDate) {
		PeriodStartDate = periodStartDate;
	}
	public String getPeriodEndDate() {
		return PeriodEndDate;
	}
	public void setPeriodEndDate(String periodEndDate) {
		PeriodEndDate = periodEndDate;
	}
	public double getGRPs() {
		return GRPs;
	}
	public void setGRPs(double gRPs) {
		GRPs = gRPs;
	}
	public double getDuration() {
		return Duration;
	}
	public void setDuration(double duration) {
		Duration = duration;
	}
	public double getContinuity() {
		return Continuity;
	}
	public void setContinuity(double continuity) {
		Continuity = continuity;
	}
	public double getReportedSpend() {
		return ReportedSpend;
	}
	public void setReportedSpend(double reportedSpend) {
		ReportedSpend = reportedSpend;
	}
	public String getLevel() {
		return Level;
	}
	public void setLevel(String level) {
		Level = level;
	}
	public double getModeledSpend() {
		return ModeledSpend;
	}
	public void setModeledSpend(double modeledSpend) {
		ModeledSpend = modeledSpend;
	}
	public double getDuetoVolume() {
		return DuetoVolume;
	}
	public void setDuetoVolume(double duetoVolume) {
		DuetoVolume = duetoVolume;
	}
	public double getProjectionFactor() {
		return ProjectionFactor;
	}
	public void setProjectionFactor(double projectionFactor) {
		ProjectionFactor = projectionFactor;
	}
	public double getVolume() {
		return Volume;
	}
	public void setVolume(double volume) {
		Volume = volume;
	}
	public double getAlpha() {
		return Alpha;
	}
	public void setAlpha(double alpha) {
		Alpha = alpha;
	}
	public double getBeta() {
		return Beta;
	}
	public void setBeta(double beta) {
		Beta = beta;
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
	public String getEventName() {
		return EventName;
	}
	public void setEventName(String eventName) {
		EventName = eventName;
	}
	public String getStudio() {
		return Studio;
	}
	public void setStudio(String studio) {
		Studio = studio;
	}
	public String getNeighborhoods() {
		return Neighborhoods;
	}
	public void setNeighborhoods(String neighborhoods) {
		Neighborhoods = neighborhoods;
	}
	public String getBU() {
		return BU;
	}
	public void setBU(String bU) {
		BU = bU;
	}
	public String getDivision() {
		return Division;
	}
	public void setDivision(String division) {
		Division = division;
	}
	public double getBASIS_PY() {
		return BASIS_PY;
	}
	public void setBASIS_PY(double bASIS_PY) {
		BASIS_PY = bASIS_PY;
	}
	public double getBASIS_P2Y() {
		return BASIS_P2Y;
	}
	public void setBASIS_P2Y(double bASIS_P2Y) {
		BASIS_P2Y = bASIS_P2Y;
	}
	public double getBASIS_P3Y() {
		return BASIS_P3Y;
	}
	public void setBASIS_P3Y(double bASIS_P3Y) {
		BASIS_P3Y = bASIS_P3Y;
	}
	public double getBASIS_Duration_PY() {
		return BASIS_Duration_PY;
	}
	public void setBASIS_Duration_PY(double bASIS_Duration_PY) {
		BASIS_Duration_PY = bASIS_Duration_PY;
	}
	public double getBASIS_Duration_P2Y() {
		return BASIS_Duration_P2Y;
	}
	public void setBASIS_Duration_P2Y(double bASIS_Duration_P2Y) {
		BASIS_Duration_P2Y = bASIS_Duration_P2Y;
	}
	public double getBASIS_Duration_P3Y() {
		return BASIS_Duration_P3Y;
	}
	public void setBASIS_Duration_P3Y(double bASIS_Duration_P3Y) {
		BASIS_Duration_P3Y = bASIS_Duration_P3Y;
	}
	public double getTypical() {
		return Typical;
	}
	public void setTypical(double typical) {
		Typical = typical;
	}
	public double getCont() {
		return Cont;
	}
	public void setCont(double cont) {
		Cont = cont;
	}
	public double getAC() {
		return AC;
	}
	public void setAC(double aC) {
		AC = aC;
	}
	public double getX1() {
		return X1;
	}
	public void setX1(double x1) {
		X1 = x1;
	}
	public double getX1_2() {
		return X1_2;
	}
	public void setX1_2(double x1_2) {
		X1_2 = x1_2;
	}
	public double getX1_3() {
		return X1_3;
	}
	public void setX1_3(double x1_3) {
		X1_3 = x1_3;
	}
	public double getX2() {
		return X2;
	}
	public void setX2(double d) {
		X2 = d;
	}
	public String getPeriodType() {
		return PeriodType;
	}
	public void setPeriodType(String periodType) {
		PeriodType = periodType;
	}
	public String getReport_Period() {
		return Report_Period;
	}
	public void setReport_Period(String report_Period) {
		Report_Period = report_Period;
	}
	public String getCatLib() {
		return CatLib;
	}
	public void setCatLib(String catLib) {
		CatLib = catLib;
	}
	public double getGamma_X1() {
		return Gamma_X1;
	}
	public void setGamma_X1(double gamma_X1) {
		Gamma_X1 = gamma_X1;
	}
	public double getGamma_X1_2() {
		return Gamma_X1_2;
	}
	public void setGamma_X1_2(double gamma_X1_2) {
		Gamma_X1_2 = gamma_X1_2;
	}
	public double getGamma_X1_3() {
		return Gamma_X1_3;
	}
	public void setGamma_X1_3(double gamma_X1_3) {
		Gamma_X1_3 = gamma_X1_3;
	}
	public double getGamma_X2() {
		return Gamma_X2;
	}
	public void setGamma_X2(double gamma_X2) {
		Gamma_X2 = gamma_X2;
	}
	
	
}

