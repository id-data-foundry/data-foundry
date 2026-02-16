package services.jsexecutor.graalsandbox.internal;

import org.graalvm.polyglot.HostAccess;

@SuppressWarnings("all")
public class GraalInterruptTest {

	@HostAccess.Export
	public void test() throws InterruptedException {
		if (Thread.interrupted()) {
			throw new InterruptedException();
		}
	}
}
