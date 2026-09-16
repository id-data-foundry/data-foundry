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
}
