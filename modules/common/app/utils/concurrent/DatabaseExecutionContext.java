package utils.concurrent;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.pekko.actor.ActorSystem;

import play.libs.concurrent.CustomExecutionContext;

@Singleton
public class DatabaseExecutionContext extends CustomExecutionContext {

	@Inject
	public DatabaseExecutionContext(ActorSystem actorSystem) {
		super(actorSystem, "database.dispatcher");
	}
}
