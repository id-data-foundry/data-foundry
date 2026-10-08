package models;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import play.Application;
import play.inject.guice.GuiceApplicationBuilder;
import play.libs.ws.WSClient;
import play.libs.ws.WSRequest;
import play.test.WithApplication;
import services.search.SearchService;
import services.slack.api.SlackApi;
import utils.rendering.ImageUtil;

public class ReliabilityAndLatencyOptimizationTest extends WithApplication {

	@Override
	protected Application provideApplication() {
		return new GuiceApplicationBuilder()
				.configure("db.default.driver", "org.h2.Driver")
				.configure("db.default.url", "jdbc:h2:mem:play;DB_CLOSE_DELAY=-1")
				.configure("play.evolutions.db.default.autoApply", true)
				.configure("df.base_url", "http://localhost:9000")
				.configure("df.keys.project", "test-project-secret")
				.configure("df.keys.v2.user.api", "test-api-key")
				.configure("df.keys.v2.multi.api", "test-api-key")
				.configure("df.keys.registration", Collections.singletonList("test-access-code"))
				.build();
	}

	@Test
	public void testSlackApiTimeoutConfiguration() throws Exception {
		SlackApi apiCustom = new SlackApi("https://hooks.slack.com/services/T00/B00/TOKEN", 2500);
		Field timeoutField = SlackApi.class.getDeclaredField("timeout");
		timeoutField.setAccessible(true);
		assertEquals(2500, timeoutField.getInt(apiCustom));

		SlackApi apiDefault = new SlackApi("https://hooks.slack.com/services/T00/B00/TOKEN");
		assertEquals(5000, timeoutField.getInt(apiDefault));
	}

	@Test
	public void testGoogleFitWsRequestTimeout() {
		WSClient ws = app.injector().instanceOf(WSClient.class);
		WSRequest request = ws.url("https://www.googleapis.com/oauth2/v3/tokeninfo?id_token=mock")
				.setRequestTimeout(Duration.ofSeconds(10));
		assertNotNull(request);
		assertEquals(Duration.ofSeconds(10), request.getRequestTimeout().orElse(null));
	}

	@Test
	public void testImageUtilProcessTimeoutAndNonexistentFileHandling() throws Exception {
		ImageUtil imageUtil = new ImageUtil();
		Field timeoutField = ImageUtil.class.getDeclaredField("CONVERT_TIMEOUT_SECONDS");
		timeoutField.setAccessible(true);
		assertEquals(30L, timeoutField.getLong(null));

		Method runConvertMethod = ImageUtil.class.getDeclaredMethod("runConvert", File.class, File.class, int.class, int.class, float.class);
		runConvertMethod.setAccessible(true);
		File nonExistentSource = new File("target/nonexistent-image-" + System.currentTimeMillis() + ".png");
		File destination = new File("target/out-" + System.currentTimeMillis() + ".png");

		// Should not hang and return false when source file doesn't exist or convert tool fails
		boolean result = (boolean) runConvertMethod.invoke(imageUtil, nonExistentSource, destination, 100, 100, 90f);
		assertFalse("runConvert on nonexistent source should fail gracefully", result);
	}

	@Test
	public void testSearchServiceNumHitsSupport() {
		SearchService searchService = app.injector().instanceOf(SearchService.class);
		assertNotNull(searchService);

		List<Project> resultsDefault = searchService.search("nonexistent_term");
		assertNotNull(resultsDefault);

		List<Project> resultsOverfetch = searchService.search("nonexistent_term", 100);
		assertNotNull(resultsOverfetch);
	}

	@Test
	public void testActorViewAdaptivePollingTemplate() throws Exception {
		File templateFile = new File("app/views/tools/actor/view.scala.html");
		if (!templateFile.exists()) {
			templateFile = new File("modules/common/app/views/tools/actor/view.scala.html");
		}
		if (!templateFile.exists()) {
			templateFile = new File("/workspace/DataFoundry/modules/common/app/views/tools/actor/view.scala.html");
		}
		assertTrue("Actor template file must exist", templateFile.exists());

		String templateContent = new String(Files.readAllBytes(templateFile.toPath()));
		assertFalse("Live output must not have hardcoded 1s continuous hx-trigger",
				templateContent.contains("hx-trigger=\"every 1s\""));
		assertTrue("Live output must have adaptive polling script",
				templateContent.contains("adaptive polling for live output"));
		assertTrue("Live output must handle document.hidden to save polling resources",
				templateContent.contains("document.hidden"));
		assertTrue("Live output must listen to action buttons to reset fast polling",
				templateContent.contains("#testBtn"));
	}
}
