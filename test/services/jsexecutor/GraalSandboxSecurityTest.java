package services.jsexecutor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import services.jsexecutor.graalsandbox.GraalSandbox;
import services.jsexecutor.graalsandbox.GraalSandboxes;

public class GraalSandboxSecurityTest {

	@Test
	public void testBasicEvaluation() throws Exception {
		GraalSandbox sandbox = GraalSandboxes.create();
		Object result = sandbox.eval("1 + 2");
		assertNotNull(result);
		assertEquals(3, ((Number) result).intValue());
	}

	public static class TestBean {
		@org.graalvm.polyglot.HostAccess.Export
		public String allowedMethod() {
			return "allowed";
		}

		public String blockedMethod() {
			return "blocked";
		}
	}

	@Test
	public void testHostAccessExplicitEnforcement() throws Exception {
		GraalSandbox sandbox = GraalSandboxes.create();
		javax.script.Bindings bindings = sandbox.createNewBindings();
		bindings.put("bean", new TestBean());

		// Exported method should execute successfully
		Object allowedResult = sandbox.eval("bean.allowedMethod()", bindings);
		assertEquals("allowed", allowedResult.toString());

		// Unexported method should fail / not be accessible under HostAccess.EXPLICIT
		try {
			sandbox.eval("bean.blockedMethod()", bindings);
			fail("Blocked method without @HostAccess.Export must not be executable");
		} catch (Exception e) {
			// Expected: TypeError or NoSuchMethodException
			assertTrue(true);
		}
	}

	@Test
	public void testJavaReflectionBlocked() {
		GraalSandbox sandbox = GraalSandboxes.create();
		try {
			// Trying to access unexported Java classes or methods must fail under HostAccess.EXPLICIT
			sandbox.eval("java.lang.System.exit(1);");
			fail("Should not allow access to System.exit");
		} catch (Exception e) {
			// Expected exception
			assertTrue(true);
		}
	}
}
