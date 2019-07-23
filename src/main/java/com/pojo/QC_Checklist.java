package com.pojo;

import java.io.Serializable;

public class QC_Checklist  implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	public String SourceBDA;
	public String getSourceBDA() {
		return SourceBDA;
	}
	public void setSourceBDA(String sourceBDA) {
		SourceBDA = sourceBDA;
	}
	public String getFileName() {
		return FileName;
	}
	public void setFileName(String fileName) {
		FileName = fileName;
	}
	public String getError_Hint() {
		return Error_Hint;
	}
	public void setError_Hint(String error_Hint) {
		Error_Hint = error_Hint;
	}
	public String getError() {
		return Error;
	}
	public void setError(String error) {
		Error = error;
	}
	public String getComments() {
		return Comments;
	}
	public void setComments(String comments) {
		Comments = comments;
	}
	public String FileName;
	public String Error_Hint;
	public String Error;
	public String Comments;

}
