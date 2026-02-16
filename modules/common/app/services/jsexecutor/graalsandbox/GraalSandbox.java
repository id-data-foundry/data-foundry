package services.jsexecutor.graalsandbox;

import javax.script.Bindings;

import delight.nashornsandbox.NashornSandbox;

public interface GraalSandbox extends NashornSandbox {

	public Bindings createNewBindings();
}
