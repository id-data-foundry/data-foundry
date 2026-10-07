package services.processing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import play.Application;
import play.inject.guice.GuiceApplicationBuilder;
import play.test.WithApplication;

public class MediaProcessingServiceTest extends WithApplication {

	private MediaProcessingService mediaProcessingService;

	@Override
	protected Application provideApplication() {
		return new GuiceApplicationBuilder().configure("db.default.driver", "org.h2.Driver")
				.configure("db.default.url", "jdbc:h2:mem:play;DB_CLOSE_DELAY=-1")
				.configure("play.evolutions.db.default.autoApply", true).build();
	}

	@Before
	public void setUp() {
		mediaProcessingService = app.injector().instanceOf(MediaProcessingService.class);
	}

	@Test
	public void testTimeoutConstants() {
		assertTrue(MediaProcessingService.PROCESS_TIMEOUT_MINUTES > 0);
		assertTrue(MediaProcessingService.FFMPEG_TIMEOUT_MINUTES > 0);
		assertTrue(MediaProcessingService.ESPEAK_TIMEOUT_SECONDS > 0);
	}

	@Test
	public void testNoPermitLeakOnMissingFile() throws Exception {
		assertEquals(0, mediaProcessingService.getEnqueuedJobs());

		File nonExistent = new File("non_existent_file_" + UUID.randomUUID().toString() + ".wav");
		CompletionStage<String> stage = mediaProcessingService.scheduleMediaToTextProcess(nonExistent, "en", "audio/wav",
				"testUser", "token123");

		String result = stage.toCompletableFuture().get(5, TimeUnit.SECONDS);
		assertEquals("[ERROR] File missing.", result);

		// Assert that the permit was released and no leak occurred
		assertEquals(0, mediaProcessingService.getEnqueuedJobs());
	}
}
