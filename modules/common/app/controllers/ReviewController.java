package controllers;

import java.util.Date;
import java.util.List;

import javax.inject.Inject;

import com.typesafe.config.Config;

import models.Dataset;
import models.LabNotesEntry;
import models.LabNotesEntry.LabNotesEntryType;
import models.Person;
import models.Project;
import play.data.DynamicForm;
import play.data.FormFactory;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.twirl.api.Html;
import services.email.NotificationService;
import utils.auth.TokenResolverUtil;
import utils.conf.ConfigurationUtils;

public class ReviewController extends AbstractAsyncController {

	private static final String REVIEWER_ID = "reviewer_id";

	private final Config config;
	private final FormFactory formFactory;
	private final NotificationService notificationService;
	private final TokenResolverUtil tokenResolverUtil;

	@Inject
	public ReviewController(Config config, FormFactory formFactory, NotificationService notificationService,
	        TokenResolverUtil tokenResolverUtil) {
		this.config = config;
		this.formFactory = formFactory;
		this.notificationService = notificationService;
		this.tokenResolverUtil = tokenResolverUtil;
	}

	public Result view(Request request, String invite_token) {

		// check invite_token
		if (invite_token == null || invite_token.length() == 0) {
			invite_token = session(request, REVIEWER_ID);
		}

		// check invite_token again
		if (invite_token == null || invite_token.length() == 0) {
			return redirect(routes.HomeController.index());
		}

		// check participant id from invite_id
		long reviewer_id = tokenResolverUtil.getReviewerIdFromReviewToken(invite_token);
		if (reviewer_id < 0) {
			return redirect(routes.HomeController.index());
		}

		// check participant object
		Person reviewer = Person.find.byId(reviewer_id);
		if (reviewer == null) {
			return redirect(routes.HomeController.index());
		}

		// check whether person is also reviewer
		List<String> reviewUsers = config.getStringList(ConfigurationUtils.DF_USERS_REVIEWERS);
		if (reviewUsers.stream().noneMatch(s -> s.equals(reviewer.getEmail()))) {
			return redirect(routes.HomeController.index());
		}

		final Project project = Project.find.byId(tokenResolverUtil.getProjectIdFromReviewToken(invite_token));
		project.refresh();

		// set token

		// review home
		return ok(views.html.review.view.render(invite_token, reviewer, project)).addingToSession(request, REVIEWER_ID,
		        invite_token);
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public Result approve(Request request, String invite_token) {
		// check invite_token
		if (invite_token == null || invite_token.length() == 0) {
			invite_token = session(request, REVIEWER_ID);
		}

		// check invite_token again
		if (invite_token == null || invite_token.length() == 0) {
			return redirect(routes.HomeController.index());
		}

		// check participant id from invite_id
		long reviewer_id = tokenResolverUtil.getReviewerIdFromReviewToken(invite_token);
		if (reviewer_id < 0) {
			return redirect(routes.HomeController.index());
		}

		// check participant object
		Person reviewer = Person.find.byId(reviewer_id);
		if (reviewer == null) {
			return redirect(routes.HomeController.index());
		}

		// check whether person is also reviewer
		List<String> adminUsers = config.getStringList(ConfigurationUtils.DF_USERS_REVIEWERS);
		if (adminUsers.stream().noneMatch(s -> s.equals(reviewer.getEmail()))) {
			return redirect(routes.HomeController.index());
		}

		final Project project = Project.find.byId(tokenResolverUtil.getProjectIdFromReviewToken(invite_token));
		project.refresh();
		project.getDatasets().stream().forEach(ds -> ds.refresh());

		DynamicForm df = formFactory.form().bindFromRequest(request);
		StringBuilder comments = new StringBuilder();
		comments.append("#Review feedback by " + reviewer.getName() + " on " + new Date());
		comments.append("\n\n");
		{
			final String gf = df.get("general-comments");
			if (gf != null && gf.length() > 1) {
				comments.append("##General feedback");
				comments.append(gf);
				comments.append("\n\n");
			}
		}
		{
			final String gf = df.get("p_" + project.getId() + "-comments");
			if (gf != null && gf.length() > 1) {
				comments.append("##Project feedback");
				comments.append(gf);
				comments.append("\n\n");
			}
		}
		for (Dataset ds : project.getDatasets()) {
			final String gf = df.get("ds_" + ds.getId() + "-comments");
			if (gf != null && gf.length() > 1) {
				comments.append("###Dataset feedback: " + ds.getName());
				comments.append(gf);
				comments.append("\n\n");
			}
		}
		{
			final String gf = df.get("participants-comments");
			if (gf != null && gf.length() > 1) {
				comments.append("###Participants feedback");
				comments.append(gf);
				comments.append("\n\n");
			}
		}

		// log an entry
		LabNotesEntry.log(ReviewController.class, LabNotesEntryType.APPROVE, comments.toString(), project);

		// inform researcher
		String email = project.getOwner().getEmail();
		String actionLink = routes.ProjectsController.view(project.getId()).absoluteURL(request, true);
		Html htmlBody = views.html.emails.invite.render("Review notification",
		        String.format("Your project review is done. " + "The reviewer does approve the current project setup. "
		                + "Please check the feedback in the comment below. "
		                + "Click the button to be forwarded to your project directly. <br/><br/>" + "FEEDBACK<br/><br/>"
		                + comments.toString()),
		        "");
		String textBody = String.format(
		        "Hello! \n\n" + "Your project review is done.\n"
		                + "The reviewer does approve the current project setup. \n"
		                + "Please check the feedback in the comment below. \n"
		                + "Click the link to be forwarded to your project directly. \n\n" + "FEEDBACK\n\n%s \n\n",
		        actionLink);

		try {
			notificationService.sendMail(email, textBody, htmlBody, actionLink, "[ID Data Foundry] Review notification",
			        reviewer.getEmail(), reviewer.getName(), "Review notification: " + actionLink);
		} catch (Exception e) {
			// play-mailer not configured?
		}

		return ok(views.html.review.thanks.render(invite_token, "Project approved",
		        "Thank you, the approval and feedback are recorded in the project and the researchers will be informed about this. Till next time."));
	}

	public Result feedback(Request request, String invite_token) {
		// check invite_token
		if (invite_token == null || invite_token.length() == 0) {
			invite_token = session(request, REVIEWER_ID);
		}

		// check invite_token again
		if (invite_token == null || invite_token.length() == 0) {
			return redirect(routes.HomeController.index());
		}

		// check participant id from invite_id
		long reviewer_id = tokenResolverUtil.getReviewerIdFromReviewToken(invite_token);
		if (reviewer_id < 0) {
			return redirect(routes.HomeController.index());
		}

		// check participant object
		Person reviewer = Person.find.byId(reviewer_id);
		if (reviewer == null) {
			return redirect(routes.HomeController.index());
		}

		// check whether person is also reviewer
		List<String> reviewUsers = config.getStringList(ConfigurationUtils.DF_USERS_REVIEWERS);
		if (reviewUsers.stream().noneMatch(s -> s.equals(reviewer.getEmail()))) {
			return redirect(routes.HomeController.index());
		}

		final Project project = Project.find.byId(tokenResolverUtil.getProjectIdFromReviewToken(invite_token));
		project.refresh();
		project.getDatasets().stream().forEach(ds -> ds.refresh());

		DynamicForm df = formFactory.form().bindFromRequest(request);
		StringBuilder comments = new StringBuilder();
		comments.append("#Review feedback by " + reviewer.getName() + " on " + new Date());
		comments.append("\n\n");
		{
			final String gf = df.get("general-comments");
			if (gf != null && gf.length() > 1) {
				comments.append("##General feedback");
				comments.append("\n");
				comments.append(gf);
				comments.append("\n\n");
			}
		}
		{
			final String gf = df.get("p_" + project.getId() + "-comments");
			if (gf != null && gf.length() > 1) {
				comments.append("##Project feedback");
				comments.append("\n");
				comments.append(gf);
				comments.append("\n\n");
			}
		}
		for (Dataset ds : project.getDatasets()) {
			final String gf = df.get("ds_" + ds.getId() + "-comments");
			if (gf != null && gf.length() > 1) {
				comments.append("###Dataset feedback: " + ds.getName());
				comments.append("\n");
				comments.append(gf);
				comments.append("\n\n");
			}
		}
		{
			final String gf = df.get("participants-comments");
			if (gf != null && gf.length() > 1) {
				comments.append("###Participants feedback");
				comments.append("\n");
				comments.append(gf);
				comments.append("\n\n");
			}
		}

		// log an entry
		LabNotesEntry.log(ReviewController.class, LabNotesEntryType.COMMENT, comments.toString(), project);

		// inform researcher
		String email = project.getOwner().getEmail();
		String actionLink = routes.ProjectsController.view(project.getId()).absoluteURL(request, true);
		Html htmlBody = views.html.emails.invite.render("Review notification",
		        String.format("Your project review is done. "
		                + "The reviewer does not yet approve and would like to suggest a few changes to the project (after which you can request a new review). "
		                + "Please check the feedback in the comment below. "
		                + "Click the button to be forwarded to your project directly. <br/><br/>" + "FEEDBACK<br/><br/>"
		                + comments.toString()),
		        "");
		String textBody = String.format("Hello! \n\n" + "Your project review is done.\n"
		        + "The reviewer does not yet approve and would like to suggest a few changes to the project (after which you can request a new review). \n"
		        + "Please check the feedback in the comment below. \n"
		        + "Click the link to be forwarded to your project directly. \n\n" + "FEEDBACK\n\n%s \n\n", actionLink);

		try {
			notificationService.sendMail(email, textBody, htmlBody, actionLink, "[ID Data Foundry] Review notification",
			        reviewer.getEmail(), reviewer.getName(), "Review notification: " + actionLink);
		} catch (Exception e) {
			// play-mailer not configured?
		}

		return ok(views.html.review.thanks.render(invite_token, "Project not yet approved",
		        "Thank you, the feedback is recorded in the project and the researchers will be informed about this. They will send a new request once the feedback is addressed."));
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////
}
