package utils.export;

public class ZenodoRecord {
	private final int id;
	private final String doi;
	private final String editUrl;
	private final String previewUrl;

	public ZenodoRecord(int id, String doi, String editUrl, String previewUrl) {
		this.id = id;
		this.doi = doi;
		this.editUrl = editUrl;
		this.previewUrl = previewUrl;
	}

	public int getId() {
		return id;
	}

	public String getDoi() {
		return doi;
	}

	public String getEditUrl() {
		return editUrl;
	}

	public String getPreviewUrl() {
		return previewUrl;
	}
}