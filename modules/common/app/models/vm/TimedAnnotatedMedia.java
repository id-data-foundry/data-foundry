package models.vm;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class TimedAnnotatedMedia extends TimedMedia {

	public List<String> annotations = new ArrayList<String>();

	public TimedAnnotatedMedia(Long id, Date timestamp, String link, String mediaType, String description) {
		this(id, timestamp, link, mediaType, description, -1L);
	}

	public TimedAnnotatedMedia(Long id, Date timestamp, String link, String mediaType, String description,
	        long datasetId) {
		super(id, timestamp, link, mediaType, description, null, datasetId);
	}

	public void addAnnotation(String annotation) {
		this.annotations.add(annotation);
	}
}
