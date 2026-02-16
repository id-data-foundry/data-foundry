package services.jsexecutor;

import java.util.concurrent.ExecutorService;

import delight.nashornsandbox.NashornSandbox;
import delight.nashornsandbox.NashornSandboxes;
import services.jsexecutor.graalsandbox.GraalSandboxes;

public class JSSandboxFactory {

	private final boolean graalJSActivated;

	public JSSandboxFactory(boolean graalJSActivated) {
		this.graalJSActivated = graalJSActivated;
	}

	public NashornSandbox createSandbox(ExecutorService EXECUTOR, long maxCPU, long maxMem, int maxStatements) {
		NashornSandbox sandbox;
		if (graalJSActivated) {
			// create a Graal JS sandbox
			sandbox = GraalSandboxes.create();
		} else {
			// create a Nashorn sandbox with ES6 support
			sandbox = NashornSandboxes.create("--language=es6");
		}

		// configure safe sandbox
		sandbox.setExecutor(EXECUTOR);
		sandbox.setMaxCPUTime(maxCPU);
		sandbox.setMaxMemory(maxMem);
		sandbox.setMaxPreparedStatements(maxStatements);

		return sandbox;
	}
}
