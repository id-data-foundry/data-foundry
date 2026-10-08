package utils.concurrent;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.pekko.actor.ActorSystem;

import play.libs.concurrent.CustomExecutionContext;

@Singleton
public class AiExecutionContext extends CustomExecutionContext {

	@Inject
	public AiExecutionContext(ActorSystem actorSystem) {
		super(actorSystem, "ai.dispatcher");
	}
}
