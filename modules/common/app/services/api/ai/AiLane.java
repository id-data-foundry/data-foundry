package services.api.ai;

import services.api.ApiServiceConstants;

/**
 * A category of AI work, carrying its own concurrency budget and deadline. Mirrors the taxonomy already used by
 * UnmanagedAIApiService.mapTaskToType().
 */
public enum AiLane {

	LLM(16, 120_000), //
	STT(4, 180_000), //
	TTS(8, 60_000), //
	IMAGE(2, 600_000), //
	EMBEDDING(4, 60_000), //
	MODELS(0, 5_000); // 0 = unlimited; served from cache

	private final int defaultConcurrency;
	private final int defaultTimeoutMs;

	AiLane(int defaultConcurrency, int defaultTimeoutMs) {
		this.defaultConcurrency = defaultConcurrency;
		this.defaultTimeoutMs = defaultTimeoutMs;
	}

	public int concurrency() {
		return defaultConcurrency;
	}

	public int timeoutMs() {
		return defaultTimeoutMs;
	}

	public static AiLane of(String task) {
		if (task == null) {
			return LLM;
		}
		switch (task) {
		case ApiServiceConstants.REQUEST_TASK_AUDIO_TRANSCRIPTION:
			return STT;
		case ApiServiceConstants.REQUEST_TASK_SPEECH_GENERATION:
			return TTS;
		case ApiServiceConstants.REQUEST_TASK_IMAGE_GENERATION:
			return IMAGE;
		case ApiServiceConstants.REQUEST_TASK_EMBEDDING:
			return EMBEDDING;
		case ApiServiceConstants.REQUEST_TASK_MODELS:
			return MODELS;
		default:
			return LLM;
		}
	}
}
