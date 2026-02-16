package services.api.remoting;

import java.io.IOException;
import java.util.Optional;

import com.fasterxml.jackson.databind.node.ObjectNode;

import controllers.api2.UnmanagedAIApiController.ChunkedWriter;

public class StreamingRemoteApiRequest extends RemoteApiRequest {

	private boolean isCompleted = false;
	private boolean isRunning = false;
	private final ChunkedWriter writer;

	public StreamingRemoteApiRequest(ChunkedWriter writer, int msTimeout, String type, String username,
	        String authorization, long projectId, ObjectNode params) throws IOException {
		super(type, msTimeout, username, authorization, projectId, params);
		this.writer = writer;
	}

	@Override
	public void cancel() {
		this.writer.close();
		super.cancel();
	}

	@Override
	public String getResult() {
		throw new RuntimeException("Not implemented, not applicable.");
	}

	@Override
	public void setResult(Optional<String> result) {
		throw new RuntimeException("Not implemented, not applicable.");
	}

	public void appendResult(String str) {
		isRunning = true;
		writer.append(str);
	}

	public void finish() {
		// if we have not delivered anything, then send an error response
		if (!isRunning) {
			writer.append(errorMessage("API timeout.").toString());
		}
		writer.close();
		isCompleted = true;
	}

	@Override
	public void timeout() {
		// cancel
		super.cancel();

		// finish off the stream
		finish();
	}

	/**
	 * check whether the API request is complete and results can be retrieved
	 * 
	 * @return
	 */
	@Override
	public boolean isCompleted() {
		return isCompleted;
	}

}
