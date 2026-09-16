package controllers.auth;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import play.mvc.Http.Request;
import play.mvc.Http.RequestBuilder;
import play.mvc.Security;

public class ApiAuthorizationSecurityTest {

	@Test
	public void testIsAuthorizedForDataset() {
		// Valid authorized dataset
		Request authorizedReq = new RequestBuilder()
				.attr(Security.USERNAME, "42")
				.build();

		assertTrue("Authorized request with matching dataset id should pass",
				DatasetApiAuth.isAuthorizedForDataset(authorizedReq, 42L));

		assertFalse("Authorized request with different dataset id should fail (cross-tenant IDOR protection)",
				DatasetApiAuth.isAuthorizedForDataset(authorizedReq, 99L));

		assertFalse("Null dataset id should fail",
				DatasetApiAuth.isAuthorizedForDataset(authorizedReq, null));

		// Unauthorized request (no username attr)
		Request unauthorizedReq = new RequestBuilder().build();
		assertFalse("Request without auth attribute should fail",
				DatasetApiAuth.isAuthorizedForDataset(unauthorizedReq, 42L));

		assertFalse("Null request should fail",
				DatasetApiAuth.isAuthorizedForDataset(null, 42L));
	}
}
