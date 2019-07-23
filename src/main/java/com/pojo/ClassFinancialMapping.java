package com.pojo;

import java.io.Serializable;

public class ClassFinancialMapping implements Serializable {

	private static final long serialVersionUID = 1L;
	public String SBU;
	public String PMU;
	public String Brand;

	public String Category;
	public String Size;
	public String Beneficiary;
	public String Material_Description;
	public String Pack_Size;
	public String FG_Material;
	public String PH5;
	public String UPC;
	public String Channel;
	

	public String getSBU() {
		return SBU;
	}

	public void setSBU(String sbu) {
		SBU = sbu;
	}

	public String getPMU() {
		return PMU;
	}

	public void setPMU(String pmu) {
		PMU = pmu;
	}
	public String getBrand() {
		return Brand;
	}

	public void setBrand(String brand) {
		Brand = brand;
	}

	public String getCategory() {
		return Category;
	}

	public void setCategory(String category) {
		Category = category;
	}

	public String getSize() {
		return Size;
	}

	public void setSize(String size) {
		Size = size;
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

	public String getMaterial_Description() {
		return Material_Description;
	}

	public void setMaterial_Description(String material_description) {
		Material_Description = material_description;
	}

	public String getPack_Size() {
		return Pack_Size;
	}

	public void setPack_Size(String pack_size) {
		Pack_Size = pack_size;
	}

	public String getFG_Material() {
		return FG_Material;
	}

	public void setFG_Material(String fg_material) {
		FG_Material = fg_material;
	}

	public String getPH5() {
		return PH5;
	}

	public void setPH5(String ph5) {
		PH5 = ph5;
	}

	public String getUPC() {
		return UPC;
	}
	
	public void setUPC(String upc) {
		UPC = upc;
	}
	
}
