package modules;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import javax.inject.Singleton;

import org.pac4j.core.client.Client;
import org.pac4j.core.client.Clients;
import org.pac4j.core.context.CallContext;
import org.pac4j.core.context.session.SessionStore;
import org.pac4j.core.credentials.Credentials;
import org.pac4j.core.credentials.UsernamePasswordCredentials;
import org.pac4j.core.credentials.authenticator.Authenticator;
import org.pac4j.core.exception.CredentialsException;
import org.pac4j.core.http.callback.NoParameterCallbackUrlResolver;
import org.pac4j.core.profile.CommonProfile;
import org.pac4j.core.util.CommonHelper;
import org.pac4j.http.client.indirect.FormClient;
import org.pac4j.oidc.client.AzureAd2Client;
import org.pac4j.oidc.client.OidcClient;
import org.pac4j.oidc.config.AzureAd2OidcConfiguration;
import org.pac4j.oidc.config.OidcConfiguration;
import org.pac4j.oidc.metadata.OidcOpMetadataResolver;
import org.pac4j.oidc.redirect.OidcRedirectionActionBuilder;
import org.pac4j.play.CallbackController;
import org.pac4j.play.LogoutController;
import org.pac4j.play.store.PlayCacheSessionStore;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;

import io.ebean.text.json.EJson;
import models.Person;
import play.Environment;
import play.cache.SyncCacheApi;
import utils.conf.ConfigurationUtils;

public class SecurityModule extends AbstractModule {

	private final Config configuration;
	private final String baseUrl;

	public SecurityModule(final Environment environment, final Config configuration) {
		if(!environment.isTest()) {
			ConfigurationUtils.checkAllConfigurations(configuration);
		}

		this.configuration = configuration;
		this.baseUrl = this.configuration.getString(ConfigurationUtils.DF_BASEURL);
	}

	@Override
	protected void configure() {
		// SSO - security configuration
		SessionStore sessionStore = new PlayCacheSessionStore(getProvider(SyncCacheApi.class)) {
			@Override
			protected void setDefaultTimeout() {
				// 12 * (1 hour = 3600 seconds)
				this.store.setTimeout(12 * 3600);
			}
		};
		bind(SessionStore.class).toInstance(sessionStore);

		// callback
		final CallbackController callbackController = new CallbackController();
		callbackController.setDefaultUrl("/");
		// plain OIDC is enabled if tenant is not defined
		if (!ConfigurationUtils.checkConfiguration(configuration, ConfigurationUtils.DF_SSO_TENANT)) {
			callbackController.setDefaultClient("OidcClient");
		} else {
			callbackController.setDefaultClient("AzureAd2Client");
		}
		callbackController.setRenewSession(true);
		bind(CallbackController.class).toInstance(callbackController);

		// logout
		final LogoutController logoutController = new LogoutController();
		logoutController.setDefaultUrl("/?logout");
		logoutController.setDestroySession(true);
		bind(LogoutController.class).toInstance(logoutController);

		// ensure that EJson loads
		// Note: do NOT remove this otherwise TelegramSessions cannot be loaded or persisted anymore
		try {
			EJson.write(new Object());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Provides
	@Singleton
	protected FormClient provideFormClient() {
		return new FormClient(baseUrl + "/login", new Authenticator() {

			@Override
			public Optional<Credentials> validate(CallContext ctx, Credentials cred) {
				if (cred == null) {
					throw new CredentialsException("No credential");
				}
				final UsernamePasswordCredentials credentials = (UsernamePasswordCredentials) cred;
				// ensure that email address is lowercase
				String username = credentials.getUsername().toLowerCase();
				String password = credentials.getPassword();
				if (CommonHelper.isBlank(username)) {
					throw new CredentialsException("Username cannot be blank");
				}
				if (CommonHelper.isBlank(password)) {
					throw new CredentialsException("Password cannot be blank");
				}

				// check given credentials in database
				Optional<Person> userOpt = Person.findByEmail(username);
				if (userOpt.isEmpty()) {
					throw new CredentialsException("User not found in system or password incorrect");
				}

				Person user = userOpt.get();
				if (!user.checkPassword(password)) {
					throw new CredentialsException("User not found in system or password incorrect");
				}

				// set the session
				user.touch();

				// create profile and store it
				final CommonProfile profile = new CommonProfile();
				profile.setId(username);
				profile.addAttribute(Person.USER_NAME, username);
				profile.addAttribute(Person.USER_ID, user.getId());
				credentials.setUserProfile(profile);

				return Optional.of(credentials);
			}

		});
	}

	@Provides
	@Singleton
	protected OidcClient provideOidcClient() {

		final OidcClient oidcClient;
		// if SSO_TENANT is defined then we are switching to AzureAD2 mode
		if (ConfigurationUtils.checkConfiguration(configuration, ConfigurationUtils.DF_SSO_TENANT)) {
			final AzureAd2OidcConfiguration oidcConfiguration = new AzureAd2OidcConfiguration();
			oidcConfiguration.setClientId(configuration.getString(ConfigurationUtils.DF_SSO_CLIENT));
			oidcConfiguration.setSecret(configuration.getString(ConfigurationUtils.DF_SSO_SECRET));
			oidcConfiguration.setDiscoveryURI(configuration.getString(ConfigurationUtils.DF_SSO_DISCOVERY));
			// specific to AzureAD
			oidcConfiguration.setTenant(configuration.getString(ConfigurationUtils.DF_SSO_TENANT));

			oidcClient = new AzureAd2Client(oidcConfiguration);
			oidcClient.setCallbackUrl(baseUrl + "/auth/callback");
			// important to configure the reply address correctly
			oidcClient.setCallbackUrlResolver(new NoParameterCallbackUrlResolver());
		}
		// else we use the plain OpenID adapter
		else {
			final OidcConfiguration oidcConfiguration = new OidcConfiguration();
			oidcConfiguration.setClientId(configuration.getString(ConfigurationUtils.DF_SSO_CLIENT));
			oidcConfiguration.setSecret(configuration.getString(ConfigurationUtils.DF_SSO_SECRET));
			oidcConfiguration.setDiscoveryURI(configuration.getString(ConfigurationUtils.DF_SSO_DISCOVERY));
			// specific to plain OIDC
			oidcConfiguration.setScope("openid");

			// no SSO configured?
			if (ConfigurationUtils.isSSO(configuration)) {
				oidcConfiguration.setOpMetadataResolver(new OidcOpMetadataResolver(oidcConfiguration));
			}

			oidcClient = new OidcClient(oidcConfiguration);
			oidcClient.setCallbackUrl(baseUrl + "/auth/callback");
			// important to configure the reply address correctly
			oidcClient.setCallbackUrlResolver(new NoParameterCallbackUrlResolver());
			oidcClient.setRedirectionActionBuilder(new OidcRedirectionActionBuilder(oidcClient));
		}

		return oidcClient;
	}

	@Provides
	@Singleton
	protected org.pac4j.core.config.Config provideConfig(OidcClient oidcClient, FormClient formClient,
	        SessionStore sessionStore) {
		List<Client> clientList = new LinkedList<>();

		// only add OIDC client if SSO is configured
		if (ConfigurationUtils.isSSO(configuration)) {
			clientList.add(oidcClient);
		}
		clientList.add(formClient);

		final Clients clients = new Clients(baseUrl + "/auth/callback", clientList);
		org.pac4j.core.config.Config config = new org.pac4j.core.config.Config(clients);
		config.setSessionStoreFactory((nfp) -> sessionStore);
		return config;
	}

	class InternalPlayCacheSessionStore extends PlayCacheSessionStore {
		@Override
		protected void setDefaultTimeout() {
			// 12 * (1 hour = 3600 seconds)
			this.store.setTimeout(12 * 3600);
		}
	}
}
