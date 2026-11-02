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
		public String sayHello(String name) {
			return "Hello, " + name;
		}
	}

	@Test
	public void testBoundObjectMethodsWork() throws Exception {
		GraalSandbox sandbox = GraalSandboxes.create();
		javax.script.Bindings bindings = sandbox.createNewBindings();
		bindings.put("bean", new TestBean());

		Object allowedResult = sandbox.eval("bean.sayHello('World')", bindings);
		assertEquals("Hello, World", allowedResult.toString());
	}

	@Test
	public void testJavaClassLookupAndReflectionBlocked() {
		GraalSandbox sandbox = GraalSandboxes.create();
		try {
			// Trying to access unexported Java classes or methods must fail because host class lookup is disabled
			sandbox.eval("java.lang.System.exit(1);");
			fail("Should not allow access to System.exit");
		} catch (Exception e) {
			// Expected: ReferenceError or TypeError
			assertTrue(true);
		}
	}

	@Test
	public void testPolyglotAccessBlocked() {
		GraalSandbox sandbox = GraalSandboxes.create();
		try {
			// Polyglot access is disabled via PolyglotAccess.NONE
			sandbox.eval("Polyglot.eval('js', '1+1');");
			fail("Polyglot.eval must not be accessible");
		} catch (Exception e) {
			// Expected
			assertTrue(true);
		}
	}

	@Test
	public void testStatementLimitTripsOnInfiniteLoop() {
		GraalSandbox sandbox = GraalSandboxes.create();
		try {
			sandbox.eval("var i = 0; while(true) { i++; }");
			fail("Infinite loop should trip statement or CPU resource limits");
		} catch (Exception e) {
			// Expected: ScriptCPUAbuseException or PolyglotException with resource limit exceeded
			assertTrue(e.getMessage() != null && (e.getMessage().toLowerCase().contains("statement") || e.getMessage().toLowerCase().contains("limit") || e.getMessage().toLowerCase().contains("cpu") || e.getMessage().toLowerCase().contains("resource")));
		}
	}

	@Test
	public void testSuccessiveEvaluationsResetLimits() throws Exception {
		GraalSandbox sandbox = GraalSandboxes.create();
		// Run multiple evaluations that each perform loops, ensuring limits are reset per execution
		for (int i = 0; i < 20; i++) {
			Object result = sandbox.eval("var count = 0; for (var j = 0; j < 500; j++) { count += j; }; count;");
			assertNotNull(result);
			assertEquals(124750, ((Number) result).intValue());
		}
	}

	@Test
	public void testStatementLimitTripsOnPaddedScriptWithManyLines() {
		GraalSandbox sandbox = GraalSandboxes.create();
		try {
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < 2500; i++) {
				sb.append("// line padding ").append(i).append("\n");
			}
			sb.append("var i = 0; while(true) { i++; }");
			sandbox.eval(sb.toString());
			fail("Padded script with >2000 lines should still trip statement limits");
		} catch (Exception e) {
			assertTrue(e instanceof delight.nashornsandbox.exceptions.ScriptCPUAbuseException
					|| (e.getMessage() != null && (e.getMessage().toLowerCase().contains("statement")
					|| e.getMessage().toLowerCase().contains("limit") || e.getMessage().toLowerCase().contains("cpu")
					|| e.getMessage().toLowerCase().contains("resource"))));
		}
	}

	@Test
	public void testStatementLimitTripsOnPaddedScriptWithLargePayload() {
		GraalSandbox sandbox = GraalSandboxes.create();
		try {
			StringBuilder sb = new StringBuilder("/* ");
			for (int i = 0; i < 60000; i++) {
				sb.append('a');
			}
			sb.append(" */\nvar i = 0; while(true) { i++; }");
			sandbox.eval(sb.toString());
			fail("Padded script with >50000 chars should still trip statement limits");
		} catch (Exception e) {
			assertTrue(e instanceof delight.nashornsandbox.exceptions.ScriptCPUAbuseException
					|| (e.getMessage() != null && (e.getMessage().toLowerCase().contains("statement")
					|| e.getMessage().toLowerCase().contains("limit") || e.getMessage().toLowerCase().contains("cpu")
					|| e.getMessage().toLowerCase().contains("resource"))));
		}
	}

	@Test
	public void testSandboxCloseDisposesContextCleanly() {
		GraalSandbox sandbox = GraalSandboxes.create();
		sandbox.close();
		// Repeated close must be safe
		sandbox.close();
	}
}
