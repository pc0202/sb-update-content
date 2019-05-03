package org.sunbird.lp.content;

public class Content {

	private String contentId;
	private String arifactUrl;
	private String downloadUrl;
	private long contentSize;

	public Content(String contentId, String artifactUrl) {
		this.contentId = contentId;
		this.arifactUrl = artifactUrl;
	}

	@Override
	public String toString() {
		return "ContentId: [" + contentId + ", arifactUrl=" + arifactUrl + ", downloadUrl=" + downloadUrl
				+ ", contentSize=" + contentSize + "]";
	}

	public String getContentId() {
		return contentId;
	}

	public void setContentId(String contentId) {
		this.contentId = contentId;
	}

	public String getArifactUrl() {
		return arifactUrl;
	}

	public void setArifactUrl(String arifactUrl) {
		this.arifactUrl = arifactUrl;
	}

	public String getDownloadUrl() {
		return downloadUrl;
	}

	public void setDownloadUrl(String downloadUrl) {
		this.downloadUrl = downloadUrl;
	}

	public long getContentSize() {
		return contentSize;
	}

	public void setContentSize(long contentSize) {
		this.contentSize = contentSize;
	}

}
