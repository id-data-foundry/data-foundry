# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v0.9.36] - Unreleased
### Security
- Set frameOptions to null and enforce frame-ancestors 'self' in CSP to support Firefox sandboxed iframes without breaking clickjacking protection ([1054db0](https://github.com/id-data-foundry/data-foundry/commit/1054db0b23df2687068ee6135260ac84cf2df915))
- Add worker-src 'self' blob:; to Starboard runner CSP ([1054db0](https://github.com/id-data-foundry/data-foundry/commit/1054db0b23df2687068ee6135260ac84cf2df915))
- Unify reference.conf with deployment filter stack (CORSModule, SecurityHeadersModule), Pekko 16k URI limits, and environment-agnostic session defaults ([1054db0](https://github.com/id-data-foundry/data-foundry/commit/1054db0b23df2687068ee6135260ac84cf2df915))
- Add StringUtils.sanitizeGanttLabel to escape reserved keywords and strip invalid Gantt characters ([a6c360e](https://github.com/id-data-foundry/data-foundry/commit/a6c360e7f26c668578918e7c4ccc237cc88c02c5))
- Update timeline.scala.html to sanitize dataset/participant labels and guard against empty sections ([a6c360e](https://github.com/id-data-foundry/data-foundry/commit/a6c360e7f26c668578918e7c4ccc237cc88c02c5))
- Add unit test suite in StringUtilsGanttSanitizeTest ([a6c360e](https://github.com/id-data-foundry/data-foundry/commit/a6c360e7f26c668578918e7c4ccc237cc88c02c5))
- Isolate web prototype views in opaque sandboxed shell (webTab.scala.html) without allow-same-origin ([add3cbf](https://github.com/id-data-foundry/data-foundry/commit/add3cbfe508b70f349f09be0ab08a95ecb636141))
- Route webToken and webViewer through sandboxed shell when ?view=tab is requested ([add3cbf](https://github.com/id-data-foundry/data-foundry/commit/add3cbfe508b70f349f09be0ab08a95ecb636141))
- Add automated test suites for bridge script escaping, multi-underscore secret keys, and sandboxed views ([add3cbf](https://github.com/id-data-foundry/data-foundry/commit/add3cbfe508b70f349f09be0ab08a95ecb636141))
- Strengthen file upload validation, add CSRF checks, and modernize Pele timeline ([dc6cd44](https://github.com/id-data-foundry/data-foundry/commit/dc6cd44e43b7aad1c40a5dad7da0d2d9805fd8fe))
- Add test suite for admin, entity table, study management, and file type security controls ([ee4bc10](https://github.com/id-data-foundry/data-foundry/commit/ee4bc10eb50f28911f0dfdd880b90e6b3c386848))
- Enforce text/plain and sandbox CSP on ScriptApiController log output ([4fb4d41](https://github.com/id-data-foundry/data-foundry/commit/4fb4d41bca5f715d8337e4cbd931e5d66649ee46))
- Return ScriptApiController.log with Content-Type: text/plain, Content-Security-Policy: sandbox, and X-Content-Type-Options: nosniff ([4fb4d41](https://github.com/id-data-foundry/data-foundry/commit/4fb4d41bca5f715d8337e4cbd931e5d66649ee46))
- Convert database backup and compaction routes to POST ([d491f16](https://github.com/id-data-foundry/data-foundry/commit/d491f16b7fcaded8775dc954490e3dfda60031b6))
- Update csrfLink.scala.html to generate POST forms with CSRF token in request body ([d491f16](https://github.com/id-data-foundry/data-foundry/commit/d491f16b7fcaded8775dc954490e3dfda60031b6))
- Enforce authentication, project ownership, and dataset validation on narrative survey image actions ([5f07c9c](https://github.com/id-data-foundry/data-foundry/commit/5f07c9c1b704f7d057bd7fd5ff43d5c87d8f8c92))
- Add @Authenticated(UserAuth.class) and @RequireCSRFCheck to uploadImage and deleteImage ([5f07c9c](https://github.com/id-data-foundry/data-foundry/commit/5f07c9c1b704f7d057bd7fd5ff43d5c87d8f8c92))
- Enforce CSRF protection on researcher media file uploads ([b3d136e](https://github.com/id-data-foundry/data-foundry/commit/b3d136ea13a2a43e482248f970e20935b39cdc62))
- Add @RequireCSRFCheck to MediaDSController.uploadFile ([b3d136e](https://github.com/id-data-foundry/data-foundry/commit/b3d136ea13a2a43e482248f970e20935b39cdc62))
- Add @AddCSRFToken to MediaDSController.view ([b3d136e](https://github.com/id-data-foundry/data-foundry/commit/b3d136ea13a2a43e482248f970e20935b39cdc62))
- Include CSRF token form field in media/view.scala.html ([b3d136e](https://github.com/id-data-foundry/data-foundry/commit/b3d136ea13a2a43e482248f970e20935b39cdc62))
- Validate study management uploads and convert removeFile to CSRF-protected POST ([74868f7](https://github.com/id-data-foundry/data-foundry/commit/74868f7c5a80a4efd6fba40e21ce4e19f8c19468))
- Replace @AddCSRFToken with @RequireCSRFCheck and @Authenticated(UserAuth.class) on studyManagementAddFile ([74868f7](https://github.com/id-data-foundry/data-foundry/commit/74868f7c5a80a4efd6fba40e21ce4e19f8c19468))
- Convert studyManagementRemoveFile from GET to POST with @RequireCSRFCheck and @Authenticated(UserAuth.class) ([74868f7](https://github.com/id-data-foundry/data-foundry/commit/74868f7c5a80a4efd6fba40e21ce4e19f8c19468))
- Update studyManagement.scala.html upload form and deletion links to submit POST with CSRF tokens ([74868f7](https://github.com/id-data-foundry/data-foundry/commit/74868f7c5a80a4efd6fba40e21ce4e19f8c19468))
- Enforce project edit access and convert table batch ingestion to CSRF-protected POST ([2d13087](https://github.com/id-data-foundry/data-foundry/commit/2d13087dfe8451ef8b5a1b07033c0808dfb93d0c))
- Convert GET /datasets/entity/table/add* routes to POST with @RequireCSRFCheck ([2d13087](https://github.com/id-data-foundry/data-foundry/commit/2d13087dfe8451ef8b5a1b07033c0808dfb93d0c))
- Update table.scala.html to issue POST requests with Csrf-Token header ([2d13087](https://github.com/id-data-foundry/data-foundry/commit/2d13087dfe8451ef8b5a1b07033c0808dfb93d0c))
- Isolate Starboard notebook in sandboxed runner iframe with postMessage bridge and storage polyfill ([1580834](https://github.com/id-data-foundry/data-foundry/commit/15808341f767a8f648d6f5ccdef8f4b8e7bf3b29))
- Expand runner CSP connect-src to allow local/remote HTTP/HTTPS/WS data fetching ([1580834](https://github.com/id-data-foundry/data-foundry/commit/15808341f767a8f648d6f5ccdef8f4b8e7bf3b29))
- Add comprehensive automated test coverage for Starboard runner CSP, legacy tokens, and public downloads ([1580834](https://github.com/id-data-foundry/data-foundry/commit/15808341f767a8f648d6f5ccdef8f4b8e7bf3b29))
- Redirect GET /datasets/existing/filePublic/:filename/*token to sandboxed /web/:token/:filename for HTML and web assets instead of forcing download ([7dc361b](https://github.com/id-data-foundry/data-foundry/commit/7dc361bdda3003f9d7883db3d42e13370d9b44d6))
- Update CompleteDSViewTest to verify legacy token HTML redirection, sandboxed webToken serving, and data file downloads ([7dc361b](https://github.com/id-data-foundry/data-foundry/commit/7dc361bdda3003f9d7883db3d42e13370d9b44d6))
- Add test suite for RFC 9116 security.txt routes and configuration overrides ([c5de6e7](https://github.com/id-data-foundry/data-foundry/commit/c5de6e734f63445ae278bd8c4d4bcfa0bfe3dd26))
- Test /.well-known/security.txt and /security.txt route handling ([c5de6e7](https://github.com/id-data-foundry/data-foundry/commit/c5de6e734f63445ae278bd8c4d4bcfa0bfe3dd26))
- Verify RFC 3339 timestamp format and default security directives ([c5de6e7](https://github.com/id-data-foundry/data-foundry/commit/c5de6e734f63445ae278bd8c4d4bcfa0bfe3dd26))
- Mitigate WebSocket CSWSH in coding agent and enforce CSRF on chatbot endpoints ([da49566](https://github.com/id-data-foundry/data-foundry/commit/da49566377c17fe95161b9186cdb3659121e4f19))
- Add cross-site WebSocket hijacking (CSWSH) mitigation in CodingAgentController.ws() by validating Origin against Host and checking fetch metadata ([da49566](https://github.com/id-data-foundry/data-foundry/commit/da49566377c17fe95161b9186cdb3659121e4f19))
- Add @RequireCSRFCheck on ChatbotController save, saveAgentsMd, and uploadFile actions ([da49566](https://github.com/id-data-foundry/data-foundry/commit/da49566377c17fe95161b9186cdb3659121e4f19))
- Provide csrfToken in file upload form in chatbot view ([da49566](https://github.com/id-data-foundry/data-foundry/commit/da49566377c17fe95161b9186cdb3659121e4f19))
- Enforce cross-project IDOR validation, upload verification, and URL scheme checks ([b95a057](https://github.com/id-data-foundry/data-foundry/commit/b95a05784934b8f2afd310d1c3529f141c6d54d1))
- Add CSRF protection to dataset configureWithToken ([b95a057](https://github.com/id-data-foundry/data-foundry/commit/b95a05784934b8f2afd310d1c3529f141c6d54d1))
- Prevent stored XSS and HTML injection across actor logs, user profiles, markdown forms, and tools ([eb4586c](https://github.com/id-data-foundry/data-foundry/commit/eb4586c4ad9d1980b5299b91fadf3d300a1ffd4d))
- Serve actor script files as text/plain with path traversal validation, nosniff, and CSP sandbox ([eb4586c](https://github.com/id-data-foundry/data-foundry/commit/eb4586c4ad9d1980b5299b91fadf3d300a1ffd4d))
- Sanitize JSON script embedding in NarrativeSurveysController, FormMarkdown, and Twine views ([eb4586c](https://github.com/id-data-foundry/data-foundry/commit/eb4586c4ad9d1980b5299b91fadf3d300a1ffd4d))
- Harden GraalVM JS sandbox with HostAccess.EXPLICIT and enforce script project ownership ([d47fcad](https://github.com/id-data-foundry/data-foundry/commit/d47fcadd540730b2808e5974ccdd6bff9a0970d7))
- Configure GraalVM Polyglot context with HostAccess.EXPLICIT sandbox policy, blocking reflection and class lookup ([d47fcad](https://github.com/id-data-foundry/data-foundry/commit/d47fcadd540730b2808e5974ccdd6bff9a0970d7))
- Add security test coverage for sandbox limits and object binding ([d47fcad](https://github.com/id-data-foundry/data-foundry/commit/d47fcadd540730b2808e5974ccdd6bff9a0970d7))
- Convert state-changing GET endpoints to CSRF-protected POST and mask tokens ([289645d](https://github.com/id-data-foundry/data-foundry/commit/289645d0e2dc3886bdafb7969ca4ecf6e53e6bbb))
- Convert state-changing GET routes to POST with @RequireCSRFCheck across UsersController, DatasetsController, and AuthApiController ([289645d](https://github.com/id-data-foundry/data-foundry/commit/289645d0e2dc3886bdafb7969ca4ecf6e53e6bbb))
- Add @AddCSRFToken to dataset form, survey, diary, and table views ([289645d](https://github.com/id-data-foundry/data-foundry/commit/289645d0e2dc3886bdafb7969ca4ecf6e53e6bbb))
- Update dataset configuration, entry point selection, and reset templates to issue POST requests with CSRF tokens ([289645d](https://github.com/id-data-foundry/data-foundry/commit/289645d0e2dc3886bdafb7969ca4ecf6e53e6bbb))
- Enforce strict origin and fetch metadata context validation in UserAuth ([8ecf73e](https://github.com/id-data-foundry/data-foundry/commit/8ecf73eec489351da1281fc53c7f4259ccfea117))
- Isolate web dataset origin with sandbox and ephemeral viewer tokens ([7024209](https://github.com/id-data-foundry/data-foundry/commit/7024209c9ad6de8e9108d6b10ef99e58ad460116))
- Remove allow-same-origin from WEB_CSP_SANDBOX to enforce opaque origin (window.origin === 'null') ([7024209](https://github.com/id-data-foundry/data-foundry/commit/7024209c9ad6de8e9108d6b10ef99e58ad460116))
- Invert Content-Security-Policy and nosniff headers unconditionally across all web assets ([7024209](https://github.com/id-data-foundry/data-foundry/commit/7024209c9ad6de8e9108d6b10ef99e58ad460116))
- Enforce Content-Disposition: attachment, application/x-download, and sandbox CSP on file downloads in CompleteDSController ([7024209](https://github.com/id-data-foundry/data-foundry/commit/7024209c9ad6de8e9108d6b10ef99e58ad460116))

### Added
- Add preprocessPeleSource in diagramHelper.scala.html to prefix conflicting task labels with \u200B ([a6c360e](https://github.com/id-data-foundry/data-foundry/commit/a6c360e7f26c668578918e7c4ccc237cc88c02c5))
- Add resilient rendering error fallback card in diagramHelper.scala.html ([a6c360e](https://github.com/id-data-foundry/data-foundry/commit/a6c360e7f26c668578918e7c4ccc237cc88c02c5))
- Add null-safety checks across AnnotationDS, SurveyDS, DiaryDS, ExpSamplingDS, MovementDS, MediaDS, FitbitDS, and GoogleFitDS controllers. ([663c9bd](https://github.com/id-data-foundry/data-foundry/commit/663c9bd984e4a9da3f2f434082e65bdeccccea14))
- Add comprehensive integration test suite DatasetDownloadTest verifying Timeseries and Entity downloads via public token, cluster filter, and authenticated owner session. ([663c9bd](https://github.com/id-data-foundry/data-foundry/commit/663c9bd984e4a9da3f2f434082e65bdeccccea14))
- Use @elements.icon for projects and datasets in access datasets helper ([690129a](https://github.com/id-data-foundry/data-foundry/commit/690129a14d041559a2c182c37c965ddbd4a58d81))
- Add Materialize green sketchnote pillar images and polish copy ([c64ab90](https://github.com/id-data-foundry/data-foundry/commit/c64ab90c7537f676b536bafd575fa9eaba8d7714))
- Sandbox notebook runner, fix CSP connect-src, and support legacy public tokens ([1580834](https://github.com/id-data-foundry/data-foundry/commit/15808341f767a8f648d6f5ccdef8f4b8e7bf3b29))
- Add backwards-compatible legacy token resolution and dual-token matching (PUBLIC_ACCESS_TOKEN and WEB_ACCESS_TOKEN) ([1580834](https://github.com/id-data-foundry/data-foundry/commit/15808341f767a8f648d6f5ccdef8f4b8e7bf3b29))
- Migrate from mermaid to pele, add diagramHelper, and scale diagrams ([6df0f34](https://github.com/id-data-foundry/data-foundry/commit/6df0f349fc62eb60d884cdddb5acd216ad72b7fc))
- Add Dataset.getEffectiveWebToken() and Dataset.hasWebOrPublicToken() helper methods ([7dc361b](https://github.com/id-data-foundry/data-foundry/commit/7dc361bdda3003f9d7883db3d42e13370d9b44d6))
- Add unit tests covering internal system key authorization and session requirement for documentation key ([3a28167](https://github.com/id-data-foundry/data-foundry/commit/3a28167c2cdbcd35ce7d6b1c55ac9d59de807e12))
- Add strict FileTypeUtils MIME and signature validation across Media, Movement, ExpSampling, and DatasetApi file upload handlers ([b95a057](https://github.com/id-data-foundry/data-foundry/commit/b95a05784934b8f2afd310d1c3529f141c6d54d1))
- Add prefix-fallback model alias resolution in LocalModelMetadata for vendor-prefixed IDs ([0a28109](https://github.com/id-data-foundry/data-foundry/commit/0a2810944c4a6118eea42b1ca40e225ce14df359))
- Add unit tests in AbstractAIApiServiceMaxTokensTest and LocalModelMetadataTest ([0a28109](https://github.com/id-data-foundry/data-foundry/commit/0a2810944c4a6118eea42b1ca40e225ce14df359))

### Changed
- Enable allowActionSpecificHeaders = true to permit custom controller framing policies ([1054db0](https://github.com/id-data-foundry/data-foundry/commit/1054db0b23df2687068ee6135260ac84cf2df915))
- Increase Nginx request rate-limiting burst capacity to 150 for editor chunk loading ([1054db0](https://github.com/id-data-foundry/data-foundry/commit/1054db0b23df2687068ee6135260ac84cf2df915))
- Append ?view=tab to 'open in tab' and 'Open' links in view.scala.html and editFile.scala.html ([add3cbf](https://github.com/id-data-foundry/data-foundry/commit/add3cbfe508b70f349f09be0ab08a95ecb636141))
- Restore algorithmic token resolution in DatasetsController.webToken with URL percent-decoding and candidate normalization ([add3cbf](https://github.com/id-data-foundry/data-foundry/commit/add3cbfe508b70f349f09be0ab08a95ecb636141))
- Deconstruct PlaygroundBridgeUtil into pure JS and provide getBridgeScriptAsJsLiteral() with \u003c escaping ([add3cbf](https://github.com/id-data-foundry/data-foundry/commit/add3cbfe508b70f349f09be0ab08a95ecb636141))
- Prevent premature </script> closing inside inline script blocks in editFile.scala.html ([add3cbf](https://github.com/id-data-foundry/data-foundry/commit/add3cbfe508b70f349f09be0ab08a95ecb636141))
- Replace dummy in-memory cluster creation (id = -1) in DatasetsController with null when cluster_id <= -1. ([663c9bd](https://github.com/id-data-foundry/data-foundry/commit/663c9bd984e4a9da3f2f434082e65bdeccccea14))
- Guard fillClusterSlots and device list scoping across TimeseriesDS, TimeseriesDSController, and EntityDS so empty cluster dummy devices (Long.MAX_VALUE) are only injected for active clusters. ([663c9bd](https://github.com/id-data-foundry/data-foundry/commit/663c9bd984e4a9da3f2f434082e65bdeccccea14))
- Update ScriptApiControllerTest with header assertions ([4fb4d41](https://github.com/id-data-foundry/data-foundry/commit/4fb4d41bca5f715d8337e4cbd931e5d66649ee46))
- Convert /admin/backupDB and /admin/shutdownCompactDB from GET to POST in common.routes ([d491f16](https://github.com/id-data-foundry/data-foundry/commit/d491f16b7fcaded8775dc954490e3dfda60031b6))
- Validate project ownership and verify that dataset belongs to the project before executing uploads or deletions ([5f07c9c](https://github.com/id-data-foundry/data-foundry/commit/5f07c9c1b704f7d057bd7fd5ff43d5c87d8f8c92))
- Enforce FileTypeUtils.looksLikeExecutableFile check and FileTypeUtils.validateAndLog on study management uploads ([74868f7](https://github.com/id-data-foundry/data-foundry/commit/74868f7c5a80a4efd6fba40e21ce4e19f8c19468))
- Enforce editableBy(username) instead of visibleFor(username) in tableAddParticipants, tableAddWearables, and tableAddDevices ([2d13087](https://github.com/id-data-foundry/data-foundry/commit/2d13087dfe8451ef8b5a1b07033c0808dfb93d0c))
- Provide modern async/await JS and Python code snippets in Starboard datasets helper ([1580834](https://github.com/id-data-foundry/data-foundry/commit/15808341f767a8f648d6f5ccdef8f4b8e7bf3b29))
- Vendor pele.min.js (v1.9.0) and remove legacy mermaid.min.js bundle ([6df0f34](https://github.com/id-data-foundry/data-foundry/commit/6df0f349fc62eb60d884cdddb5acd216ad72b7fc))
- Introduce diagramHelper.scala.html with Materialize theming and CSS variables ([6df0f34](https://github.com/id-data-foundry/data-foundry/commit/6df0f349fc62eb60d884cdddb5acd216ad72b7fc))
- Clean up styles to remove .mermaid CSS selectors in favor of .pele ([6df0f34](https://github.com/id-data-foundry/data-foundry/commit/6df0f349fc62eb60d884cdddb5acd216ad72b7fc))
- Scale diagrams to 100% container width with dynamic useWidth calculation ([6df0f34](https://github.com/id-data-foundry/data-foundry/commit/6df0f349fc62eb60d884cdddb5acd216ad72b7fc))
- Reduce Gantt chart task bars to 60% height (14px) and update timeline row height ([6df0f34](https://github.com/id-data-foundry/data-foundry/commit/6df0f349fc62eb60d884cdddb5acd216ad72b7fc))
- Update all 16 template views to use @elements.diagramHelper() and class="pele" ([6df0f34](https://github.com/id-data-foundry/data-foundry/commit/6df0f349fc62eb60d884cdddb5acd216ad72b7fc))
- Retain mermaidHelper.scala.html as a backward-compatibility forwarder ([6df0f34](https://github.com/id-data-foundry/data-foundry/commit/6df0f349fc62eb60d884cdddb5acd216ad72b7fc))
- Allow datasets with legacy PUBLIC_ACCESS_TOKEN to open and render HTML prototypes in the browser ([7dc361b](https://github.com/id-data-foundry/data-foundry/commit/7dc361bdda3003f9d7883db3d42e13370d9b44d6))
- Keep forcing attachment download for non-web and data files (e.g. CSV, ZIP) via downloadFilePublic ([7dc361b](https://github.com/id-data-foundry/data-foundry/commit/7dc361bdda3003f9d7883db3d42e13370d9b44d6))
- Accept either WEB_ACCESS_TOKEN or PUBLIC_ACCESS_TOKEN in DatasetsController.webToken ([7dc361b](https://github.com/id-data-foundry/data-foundry/commit/7dc361bdda3003f9d7883db3d42e13370d9b44d6))
- Update complete dataset views, study/dashboard views, narrative surveys, and admin public websites to use effective web token ([7dc361b](https://github.com/id-data-foundry/data-foundry/commit/7dc361bdda3003f9d7883db3d42e13370d9b44d6))
- Verify explicit configuration overrides via GuiceApplicationBuilder ([c5de6e7](https://github.com/id-data-foundry/data-foundry/commit/c5de6e734f63445ae278bd8c4d4bcfa0bfe3dd26))
- Create internal unexposed systemKey in UnmanagedAIApiService dedicated for JVM-internal agent calls (CodingAgentController, ChatbotController, and refresh probes) ([3a28167](https://github.com/id-data-foundry/data-foundry/commit/3a28167c2cdbcd35ce7d6b1c55ac9d59de807e12))
- Authorize internal systemKey calls in UnmanagedAIApiController without requiring a browser session cookie ([3a28167](https://github.com/id-data-foundry/data-foundry/commit/3a28167c2cdbcd35ce7d6b1c55ac9d59de807e12))
- Require active authenticated user session when authorizing the documentation API key ([3a28167](https://github.com/id-data-foundry/data-foundry/commit/3a28167c2cdbcd35ce7d6b1c55ac9d59de807e12))
- Update CodingAgentController and ChatbotController to authenticate to local AI proxy using internalSystemKey ([3a28167](https://github.com/id-data-foundry/data-foundry/commit/3a28167c2cdbcd35ce7d6b1c55ac9d59de807e12))
- Check project ownership on Participant, Device, Wearable, and Cluster IDs in DatasetApiController ([b95a057](https://github.com/id-data-foundry/data-foundry/commit/b95a05784934b8f2afd310d1c3529f141c6d54d1))
- Reject foreign devices in addRecord unless open participation is explicitly enabled ([b95a057](https://github.com/id-data-foundry/data-foundry/commit/b95a05784934b8f2afd310d1c3529f141c6d54d1))
- Restrict linked dataset target URLs to http, https, and relative paths to prevent javascript: pseudo-protocol navigation ([b95a057](https://github.com/id-data-foundry/data-foundry/commit/b95a05784934b8f2afd310d1c3529f141c6d54d1))
- Restrict Telegram instant messaging to participants registered in the respective project ([b95a057](https://github.com/id-data-foundry/data-foundry/commit/b95a05784934b8f2afd310d1c3529f141c6d54d1))
- HTML-escape actor console logs, error messages, and script exception outputs in JSActor and JSActorProxy ([eb4586c](https://github.com/id-data-foundry/data-foundry/commit/eb4586c4ad9d1980b5299b91fadf3d300a1ffd4d))
- Enable HTML filtering and link URL scheme validation in FormMarkdown ([eb4586c](https://github.com/id-data-foundry/data-foundry/commit/eb4586c4ad9d1980b5299b91fadf3d300a1ffd4d))
- Escape user profile inputs in UserApiController.editUser and UsersController.bulkTeamsMe ([eb4586c](https://github.com/id-data-foundry/data-foundry/commit/eb4586c4ad9d1980b5299b91fadf3d300a1ffd4d))
- Render assistantStartPrompt with safe MarkdownRenderer in ChatbotController ([eb4586c](https://github.com/id-data-foundry/data-foundry/commit/eb4586c4ad9d1980b5299b91fadf3d300a1ffd4d))
- Strip dangerous tags including SVG/MATH and use textContent in codingagent toast notifications ([eb4586c](https://github.com/id-data-foundry/data-foundry/commit/eb4586c4ad9d1980b5299b91fadf3d300a1ffd4d))
- Export script API methods in JSDatasetProxy via @HostAccess.Export ([d47fcad](https://github.com/id-data-foundry/data-foundry/commit/d47fcadd540730b2808e5974ccdd6bff9a0970d7))
- Require project ownership in ScriptApiController.add to match ActorController.add ([d47fcad](https://github.com/id-data-foundry/data-foundry/commit/d47fcadd540730b2808e5974ccdd6bff9a0970d7))
- Implement authenticated POST /users/revealToken endpoint and mask plaintext tokens in apiAccess.scala.html ([289645d](https://github.com/id-data-foundry/data-foundry/commit/289645d0e2dc3886bdafb7969ca4ecf6e53e6bbb))
- Reject requests with null Origin or host mismatch in UserAuth.isValidRequestContext ([8ecf73e](https://github.com/id-data-foundry/data-foundry/commit/8ecf73eec489351da1281fc53c7f4259ccfea117))
- Validate Fetch Metadata (Sec-Fetch-Site, Sec-Fetch-Mode, Sec-Fetch-Dest) to prevent cross-site subresource exploitation ([8ecf73e](https://github.com/id-data-foundry/data-foundry/commit/8ecf73eec489351da1281fc53c7f4259ccfea117))
- Enforce document/iframe navigation checks when Referer originates from hosted web datasets ([8ecf73e](https://github.com/id-data-foundry/data-foundry/commit/8ecf73eec489351da1281fc53c7f4259ccfea117))
- Integrate request context validation into AbstractAsyncController.getAuthenticatedUser and getAuthenticatedUserName ([8ecf73e](https://github.com/id-data-foundry/data-foundry/commit/8ecf73eec489351da1281fc53c7f4259ccfea117))
- Implement AES-GCM encrypted ephemeral viewer tokens via TokenResolverUtil and /web/view routes ([7024209](https://github.com/id-data-foundry/data-foundry/commit/7024209c9ad6de8e9108d6b10ef99e58ad460116))
- Provide in-memory localStorage/sessionStorage polyfill and console postMessage bridge via PlaygroundBridgeUtil ([7024209](https://github.com/id-data-foundry/data-foundry/commit/7024209c9ad6de8e9108d6b10ef99e58ad460116))
- Implement extractSafeFileName with URL decoding, null safety on root paths, and directory traversal rejection ([7024209](https://github.com/id-data-foundry/data-foundry/commit/7024209c9ad6de8e9108d6b10ef99e58ad460116))
- Strip non-function message 'name' fields in AbstractAIApiService to comply with strict upstream chat templates (e.g. NInfer) ([0a28109](https://github.com/id-data-foundry/data-foundry/commit/0a2810944c4a6118eea42b1ca40e225ce14df359))
- Capture and log upstream error response bodies in UnmanagedAIApiService during SSE stream setup ([0a28109](https://github.com/id-data-foundry/data-foundry/commit/0a2810944c4a6118eea42b1ca40e225ce14df359))

### Performance
- Targeted cover image query and eager fetch search owner ([82f4ec4](https://github.com/id-data-foundry/data-foundry/commit/82f4ec41b064a3de8b6708f1388fd992728250f8))

### Removed
- Remove unused and vulnerable ProjectsController.web static file serving logic and dead routes ([b95a057](https://github.com/id-data-foundry/data-foundry/commit/b95a05784934b8f2afd310d1c3529f141c6d54d1))
- Remove redundant user message participant names and surface agent error notifications in CodingAgentController ([0a28109](https://github.com/id-data-foundry/data-foundry/commit/0a2810944c4a6118eea42b1ca40e225ce14df359))

### Fixed
- Resolve browser compatibility issues and unify reference.conf ([1054db0](https://github.com/id-data-foundry/data-foundry/commit/1054db0b23df2687068ee6135260ac84cf2df915))
- Sanitize Gantt task titles and preprocess Pele source to prevent directive collisions ([a6c360e](https://github.com/id-data-foundry/data-foundry/commit/a6c360e7f26c668578918e7c4ccc237cc88c02c5))
- Sandboxed web tab viewing, resilient token decoding, and bridge script escaping ([add3cbf](https://github.com/id-data-foundry/data-foundry/commit/add3cbfe508b70f349f09be0ab08a95ecb636141))
- Fix legacy suffix key extraction in TokenResolverUtil using lastIndexOf('_') and restore Base64 padding ([add3cbf](https://github.com/id-data-foundry/data-foundry/commit/add3cbfe508b70f349f09be0ab08a95ecb636141))
- Prevent NullPointerException on unclustered participant and device downloads ([e40486c](https://github.com/id-data-foundry/data-foundry/commit/e40486cf84c77fbfcfd8490d02d13ecd697cc762))
- Return full data on unclustered CSV download and prevent dummy cluster isolation filtering ([663c9bd](https://github.com/id-data-foundry/data-foundry/commit/663c9bd984e4a9da3f2f434082e65bdeccccea14))
- Support legacy public tokens in web views and redirect filePublic HTML to sandboxed webToken ([7dc361b](https://github.com/id-data-foundry/data-foundry/commit/7dc361bdda3003f9d7883db3d42e13370d9b44d6))
- Introduce internal system key for JVM agent calls and enforce session on documentation key ([3a28167](https://github.com/id-data-foundry/data-foundry/commit/3a28167c2cdbcd35ce7d6b1c55ac9d59de807e12))
- Resolve upstream 400 error by stripping participant message names and adding alias fallback ([0a28109](https://github.com/id-data-foundry/data-foundry/commit/0a2810944c4a6118eea42b1ca40e225ce14df359))

## [v0.9.35] - 2026-10-12
### Security
- Isolate participant dataset exports & prevent cross-participant data exfiltration ([2d29131](https://github.com/id-data-foundry/data-foundry/commit/2d29131513c9d395ed6ccf7235993d9230c01658))
- Add ParticipationApiControllerSecurityTest covering CSV and JSON isolation, 403 denials, and device scoping. ([2d29131](https://github.com/id-data-foundry/data-foundry/commit/2d29131513c9d395ed6ccf7235993d9230c01658))

### Added
- Add testTimeseriesExportChunkBatching in DatasetTest verifying chunk reduction by >99% and complete data integrity ([7d8850a](https://github.com/id-data-foundry/data-foundry/commit/7d8850a011e1d8225dbe303f27e55864f2b6896e))
- Add testItemCountAndQuantityCaching unit test in DatasetTest verifying cache hits and invalidation ([df8a77a](https://github.com/id-data-foundry/data-foundry/commit/df8a77a417da8d4db349a611720294ac1c3126b3))
- Add redirectErrorStream(true) and timed waitFor with destroyForcibly() across MediaProcessingService subprocesses; fix permit leak on missing files (Issue E5) ([e076946](https://github.com/id-data-foundry/data-foundry/commit/e076946ecc5f3fc7f0bae476f0c76ce283177a65))
- Add MediaProcessingServiceTest covering timeout constants and permit leak fix ([e076946](https://github.com/id-data-foundry/data-foundry/commit/e076946ecc5f3fc7f0bae476f0c76ce283177a65))
- Add off-peak dataset schema migration to DatabaseBackupService ([696cd0d](https://github.com/id-data-foundry/data-foundry/commit/696cd0db6b75b5e561dedab5afe501a32aa94348))
- Add adminDatasetMigration() and force override for on-demand triggers ([696cd0d](https://github.com/id-data-foundry/data-foundry/commit/696cd0db6b75b5e561dedab5afe501a32aa94348))
- Add DatabaseBackupServiceTest covering execution, skip logic, and admin triggers ([696cd0d](https://github.com/id-data-foundry/data-foundry/commit/696cd0db6b75b5e561dedab5afe501a32aa94348))
- Add (ts) and (device_id, ts) indexes in TimeseriesDS createInstance and migrateDatasetSchema (Issue A2) ([2630b0b](https://github.com/id-data-foundry/data-foundry/commit/2630b0b8758e583cb022ba7454c043eb6afadcb0))
- Add (ts) index in CompleteDS and EntityDS ([2630b0b](https://github.com/id-data-foundry/data-foundry/commit/2630b0b8758e583cb022ba7454c043eb6afadcb0))
- Add (participant_id, ts), (file_name), and (ts) indexes and implement migrateDatasetSchema in MediaDS, MovementDS, and ExpSamplingDS ([2630b0b](https://github.com/id-data-foundry/data-foundry/commit/2630b0b8758e583cb022ba7454c043eb6afadcb0))
- Add testDynamicDatasetIndexesAndMigration to verify index creation and idempotent migration ([2630b0b](https://github.com/id-data-foundry/data-foundry/commit/2630b0b8758e583cb022ba7454c043eb6afadcb0))
- Add unit tests for TimeseriesDS queue tracking and EntityDS projection update guard ([92a3182](https://github.com/id-data-foundry/data-foundry/commit/92a3182464ad6a0f8e579d54ee500c2e1824e015))
- Add null safety checks for getProject() != null prior to delegation ([647782b](https://github.com/id-data-foundry/data-foundry/commit/647782b50d6bd8658588066e97bb4e78c856545c))
- Add unit tests for Dataset permission delegation and Project.isActive ([647782b](https://github.com/id-data-foundry/data-foundry/commit/647782b50d6bd8658588066e97bb4e78c856545c))

### Changed
- TimeseriesDS, FitbitDS, GoogleFitDS: enforce WHERE ... IN (...) filtering with cluster device/wearable IDs or fallback to empty condition (WHERE 1 = 0) when no devices/wearables belong to the participant, preventing unfiltered full-table exfiltration. ([2d29131](https://github.com/id-data-foundry/data-foundry/commit/2d29131513c9d395ed6ccf7235993d9230c01658))
- EntityDS: implement resourceIdWhereClause to strictly isolate records by participant refId in exports and queries. ([2d29131](https://github.com/id-data-foundry/data-foundry/commit/2d29131513c9d395ed6ccf7235993d9230c01658))
- AnnotationDS: filter export by cluster_id when cluster_id > -1. ([2d29131](https://github.com/id-data-foundry/data-foundry/commit/2d29131513c9d395ed6ccf7235993d9230c01658))
- EntityDSController & DatasetsController: propagate cluster context through downloadExternal and downloadInternal. ([2d29131](https://github.com/id-data-foundry/data-foundry/commit/2d29131513c9d395ed6ccf7235993d9230c01658))
- ParticipationApiController: populate synthetic cluster with participant devices/wearables and fill slots; reject participant exports of project-level datasets (FORM, COMPLETE, LINKED, ANNOTATION) with 403 Forbidden; scope JSON export to participant cluster. ([2d29131](https://github.com/id-data-foundry/data-foundry/commit/2d29131513c9d395ed6ccf7235993d9230c01658))
- Configure queueSize=8192, discardingThreshold=0, and neverBlock=true on ASYNCFILE and ASYNCSTDOUT to prevent worker thread blocking (Issue D4) ([9e7dae7](https://github.com/id-data-foundry/data-foundry/commit/9e7dae793a4ee4fa65247a1450830e47f8fd54d6))
- Set default logging levels for controllers, services, and utils to INFO ([9e7dae7](https://github.com/id-data-foundry/data-foundry/commit/9e7dae793a4ee4fa65247a1450830e47f8fd54d6))
- Replace findList() with findEachWhile() in DataExportController.internalExport to prevent heap exhaustion on up to 1,000,000 rows (Issue C2) ([7d8850a](https://github.com/id-data-foundry/data-foundry/commit/7d8850a011e1d8225dbe303f27e55864f2b6896e))
- Batch row offers into 256-row ByteString chunks in DataExportController and TimeseriesDS (Issue C1) ([7d8850a](https://github.com/id-data-foundry/data-foundry/commit/7d8850a011e1d8225dbe303f27e55864f2b6896e))
- Cache Dataset.getItemCount and getItemQuantity (120s TTL) using SyncCacheApi (Issue A5) ([df8a77a](https://github.com/id-data-foundry/data-foundry/commit/df8a77a417da8d4db349a611720294ac1c3126b3))
- Invalidate dataset table count/quantity cache upon batch commit in TimeseriesDS.flushQueue ([df8a77a](https://github.com/id-data-foundry/data-foundry/commit/df8a77a417da8d4db349a611720294ac1c3126b3))
- Cache timeseries CSV aggregation in LinkedDS.timeseries (300s TTL) and deliver as single ByteString (Issue A8) ([df8a77a](https://github.com/id-data-foundry/data-foundry/commit/df8a77a417da8d4db349a611720294ac1c3126b3))
- Align cache TTL in ResourcesController device, participant, cluster, and wearable endpoints to 60s ([df8a77a](https://github.com/id-data-foundry/data-foundry/commit/df8a77a417da8d4db349a611720294ac1c3126b3))
- Release JSActor trialExecutionPermission immediately; move perceived 800ms delay to ActorController via non-blocking delayedExecutor (Issue E1) ([e076946](https://github.com/id-data-foundry/data-foundry/commit/e076946ecc5f3fc7f0bae476f0c76ce283177a65))
- Convert UnmanagedAIApiService liveness probe from blocking .get(120s) to non-blocking thenAccept with 10s probe timeout (Issue F2) ([e076946](https://github.com/id-data-foundry/data-foundry/commit/e076946ecc5f3fc7f0bae476f0c76ce283177a65))
- Run performDatasetMigration() during nightly maintenance refresh() (01:01:01 AM) ([696cd0d](https://github.com/id-data-foundry/data-foundry/commit/696cd0db6b75b5e561dedab5afe501a32aa94348))
- Guard migration with Semaphore(1) to prevent concurrent execution ([696cd0d](https://github.com/id-data-foundry/data-foundry/commit/696cd0db6b75b5e561dedab5afe501a32aa94348))
- Use AtomicBoolean migrationCompleted flag so subsequent nightly runs skip in 0ms ([696cd0d](https://github.com/id-data-foundry/data-foundry/commit/696cd0db6b75b5e561dedab5afe501a32aa94348))
- Log progress every 200 datasets for observability on large databases ([696cd0d](https://github.com/id-data-foundry/data-foundry/commit/696cd0db6b75b5e561dedab5afe501a32aa94348))
- Standardize all index names to <tableName>_<column(s)>_idx ([2630b0b](https://github.com/id-data-foundry/data-foundry/commit/2630b0b8758e583cb022ba7454c043eb6afadcb0))
- Replace ConcurrentLinkedQueue.size() traversal with AtomicInteger in TimeseriesDS, eliminating O(N^2) quadratic degradation during bulk record insertion (Issue G1) ([92a3182](https://github.com/id-data-foundry/data-foundry/commit/92a3182464ad6a0f8e579d54ee500c2e1824e015))
- Guard EntityDS updateProjection with (changed || !containsKey), preventing redundant UPDATE dataset queries and bean cache evictions on every entity write (Issue A9) ([92a3182](https://github.com/id-data-foundry/data-foundry/commit/92a3182464ad6a0f8e579d54ee500c2e1824e015))
- Drop getProject().refresh() from 7 Dataset permission check methods (belongsTo, collaboratesWith, subscribedBy, visibleFor, editableBy), preventing Ebean bean cache evictions and N+1 queries per render and HTMX poll ([647782b](https://github.com/id-data-foundry/data-foundry/commit/647782b50d6bd8658588066e97bb4e78c856545c))

### Performance
- Share GraalVM Engine, coalesce metrics in AnalyticsService, decouple RAG embeddings, make SearchService volatile (E6, F1, E8, F3) ([d8aaa6a](https://github.com/id-data-foundry/data-foundry/commit/d8aaa6a6128b4b279249d08c76121a000f6a9a85))
- Dispatch JS DB API on DatabaseExecutionContext & optimize actor proxy graph and log I/O (E2, E3) ([1c20c5d](https://github.com/id-data-foundry/data-foundry/commit/1c20c5d2cce7c5d9abc7d335a84645a88cf6e8df))
- Decouple OOCSI telemetry lock & batch dataset export loops (D5, G2) ([e185beb](https://github.com/id-data-foundry/data-foundry/commit/e185bebec6b6773124ed84641982a1d3f06e6f36))
- Subprocess timeouts, socket read timeouts, adaptive actor polling, search overfetch (C7, F6, F5, B3, F4) ([e5428d6](https://github.com/id-data-foundry/data-foundry/commit/e5428d699a7c841f095f2dcc99b2734c09b40029))
- Folder snapshot + batched participants for media listings; dedicated ai.dispatcher for chatbot (C6, D2) ([7a43496](https://github.com/id-data-foundry/data-foundry/commit/7a43496e94ce04262674b63a46b83352cd067d25))
- Eliminate timeline N+1 queries and remove redundant refresh calls across models and services (Issues A4, F3, G4) ([c15ac05](https://github.com/id-data-foundry/data-foundry/commit/c15ac05849f71ab5eacf7558ac0322ba85b8b6b9))
- Enforce export limits and 256-row batching across dataset models (Issues C5, C1) ([152cd50](https://github.com/id-data-foundry/data-foundry/commit/152cd505571721b4d19320dd88eb3bc5a3be681b))
- Eliminate 2.78MB blocking mermaid JS, lazy-load with progress indicator (Issue B1) ([3d90537](https://github.com/id-data-foundry/data-foundry/commit/3d905374722508668103b03f4076f37cab7726eb))
- Cache project approval and last-updated queries with eviction on log (Issue A6) ([dce6b2a](https://github.com/id-data-foundry/data-foundry/commit/dce6b2af8785a5584294cfd7f027e49cf548a862))
- Eliminate async log queue stalls and reduce verbosity ([9e7dae7](https://github.com/id-data-foundry/data-foundry/commit/9e7dae793a4ee4fa65247a1450830e47f8fd54d6))
- Optimize export streaming memory and batch row chunking ([7d8850a](https://github.com/id-data-foundry/data-foundry/commit/7d8850a011e1d8225dbe303f27e55864f2b6896e))
- Cache dataset counts, weekly quantities, and visualization aggregations ([df8a77a](https://github.com/id-data-foundry/data-foundry/commit/df8a77a417da8d4db349a611720294ac1c3126b3))
- Eliminate thread-pool stalls and artificial delays ([e076946](https://github.com/id-data-foundry/data-foundry/commit/e076946ecc5f3fc7f0bae476f0c76ce283177a65))
- Add indexes on ts and identifier columns for dynamic dataset tables ([2630b0b](https://github.com/id-data-foundry/data-foundry/commit/2630b0b8758e583cb022ba7454c043eb6afadcb0))
- Replace O(N) queue.size() with AtomicInteger and guard EntityDS updates ([92a3182](https://github.com/id-data-foundry/data-foundry/commit/92a3182464ad6a0f8e579d54ee500c2e1824e015))
- Eliminate redundant refresh() in Dataset permissions and Project.isActive ([647782b](https://github.com/id-data-foundry/data-foundry/commit/647782b50d6bd8658588066e97bb4e78c856545c))
- Resolves Issues A1 and A7 from PERFORMANCE.md ([647782b](https://github.com/id-data-foundry/data-foundry/commit/647782b50d6bd8658588066e97bb4e78c856545c))

### Removed
- Remove Thread.sleep(2000) from databaseExecutionContext in ProjectsController; preserve frontend progress bar delay via non-blocking delayedExecutor (Issue G4) ([e076946](https://github.com/id-data-foundry/data-foundry/commit/e076946ecc5f3fc7f0bae476f0c76ce283177a65))
- Remove redundant d.refresh() in Project.isActive() for null start dates ([647782b](https://github.com/id-data-foundry/data-foundry/commit/647782b50d6bd8658588066e97bb4e78c856545c))

## [v0.9.34] - 2026-10-12
### Security
- Add automated test coverage in ChatbotControllerSecurityTest and ChatbotMemoryUtilsTest ([99cca9d](https://github.com/id-data-foundry/data-foundry/commit/99cca9d4f05938275c5ca73df5e93eaeb7d2352f))
- Add cleanAgentOutput to sanitize thinking tags, tool call markers, and internal filesystem paths ([8194302](https://github.com/id-data-foundry/data-foundry/commit/8194302ce24c5dee230013c40d7abb56b66674be))
- Add unit tests in ChatbotControllerSecurityTest and CodingAgentUtilsTest for empty-output fallbacks, row structure, and tag sanitization ([8194302](https://github.com/id-data-foundry/data-foundry/commit/8194302ce24c5dee230013c40d7abb56b66674be))
- Reset resource limits prior to sandboxed operation execution ([7f2bca7](https://github.com/id-data-foundry/data-foundry/commit/7f2bca76786a4c971ea792f5fb6dd289c73e8a1c))
- Add @AddCSRFToken to ActorController.view and @RequireCSRFCheck to execute, save, and install ([cecf31c](https://github.com/id-data-foundry/data-foundry/commit/cecf31c19441cd559cfc885aedd29edb27fbf841))
- Migrate actor frontend view from jQuery $.ajax to htmx.ajax with CSRF header ([cecf31c](https://github.com/id-data-foundry/data-foundry/commit/cecf31c19441cd559cfc885aedd29edb27fbf841))
- Implement constant-time token comparison across controllers and models (DF-40) ([e857231](https://github.com/id-data-foundry/data-foundry/commit/e857231bb193329c23de0108066a53dda6401b01))
- Enforce framing, network controls, and popup support in dataset web CSP ([2aa678f](https://github.com/id-data-foundry/data-foundry/commit/2aa678f63eb0008cdf7f0b4147eee6a25b480796))
- Add allow-popups to WEB_CSP_SANDBOX so prototypes can open new tabs/windows (inheriting sandbox restrictions) ([2aa678f](https://github.com/id-data-foundry/data-foundry/commit/2aa678f63eb0008cdf7f0b4147eee6a25b480796))
- Introduce WEB_TOKEN_CSP_HEADER for public webToken routes, omitting frame-ancestors to permit embedding in external portfolio and study sites ([2aa678f](https://github.com/id-data-foundry/data-foundry/commit/2aa678f63eb0008cdf7f0b4147eee6a25b480796))
- Update CompleteDSViewTest to verify all new CSP directives across authenticated and token endpoints ([2aa678f](https://github.com/id-data-foundry/data-foundry/commit/2aa678f63eb0008cdf7f0b4147eee6a25b480796))

### Added
- Enhance chat view UI, transcript loading, session pruning, and memory dialog layout ([99cca9d](https://github.com/id-data-foundry/data-foundry/commit/99cca9d4f05938275c5ca73df5e93eaeb7d2352f))
- Add routes for /favicon.ico, /apple-touch-icon.png, and /apple-touch-icon-precomposed.png ([c413c84](https://github.com/id-data-foundry/data-foundry/commit/c413c8465afd24ddeef06f275ad9b252d7052406))
- Add IconRoutingTest suite verifying 200 OK for icons and 404 behavior ([c413c84](https://github.com/id-data-foundry/data-foundry/commit/c413c8465afd24ddeef06f275ad9b252d7052406))
- Align UI styling, dynamic workflow diagram, and titlebar ([4e71074](https://github.com/id-data-foundry/data-foundry/commit/4e710740126c523ad3d9b287cddcf6e08af029c0))
- Add accordion toggle listener to cleanly re-render Mermaid diagram ([4e71074](https://github.com/id-data-foundry/data-foundry/commit/4e710740126c523ad3d9b287cddcf6e08af029c0))
- Redesign admin view with tabbed layout and Materialize standards ([805ef3b](https://github.com/id-data-foundry/data-foundry/commit/805ef3b4fff896ff78696939a94f97352dca484e))
- Add interactive engine switcher between Standard and Agentic modes, ([805ef3b](https://github.com/id-data-foundry/data-foundry/commit/805ef3b4fff896ff78696939a94f97352dca484e))
- Implement agentic memory, RAG tools, output sanitization, and test view layout ([8194302](https://github.com/id-data-foundry/data-foundry/commit/8194302ce24c5dee230013c40d7abb56b66674be))
- Add live user memory inspection partial (userMemory.scala.html) and UI activity chips in chat views ([8194302](https://github.com/id-data-foundry/data-foundry/commit/8194302ce24c5dee230013c40d7abb56b66674be))
- Update local-ai.js defaults to use placeholder models and update documentation submodule ([9a59bd6](https://github.com/id-data-foundry/data-foundry/commit/9a59bd6e37007d7d5f9e54abe56da26bae694806))
- Support model placeholder names in UnmanagedAIApiController and LocalModelMetadata ([e109207](https://github.com/id-data-foundry/data-foundry/commit/e109207f514fbc2cc2e6ad53188410406a0b002a))
- Add unit tests verifying loop termination and reset per execution ([7f2bca7](https://github.com/id-data-foundry/data-foundry/commit/7f2bca76786a4c971ea792f5fb6dd289c73e8a1c))
- Add Hash.constantTimeEquals using MessageDigest.isEqual ([e857231](https://github.com/id-data-foundry/data-foundry/commit/e857231bb193329c23de0108066a53dda6401b01))
- Add unit test suite in AuthCryptoTest ([e857231](https://github.com/id-data-foundry/data-foundry/commit/e857231bb193329c23de0108066a53dda6401b01))
- Add DatabaseExecutionContext bound to database.dispatcher ([7f9c5ab](https://github.com/id-data-foundry/data-foundry/commit/7f9c5ab5ebf18782ae92ccb44daadd8a9160a592))
- Add frame-ancestors 'self' to authenticated dataset web views in DatasetsController to prevent clickjacking and UI redressing ([2aa678f](https://github.com/id-data-foundry/data-foundry/commit/2aa678f63eb0008cdf7f0b4147eee6a25b480796))
- Add connect-src 'self' https: wss: to block cleartext network exfiltration while supporting secure cloud APIs and IoT WebSockets ([2aa678f](https://github.com/id-data-foundry/data-foundry/commit/2aa678f63eb0008cdf7f0b4147eee6a25b480796))
- Add form-action 'self' and base-uri 'self' to mitigate credential phishing forms and base URL hijacking ([2aa678f](https://github.com/id-data-foundry/data-foundry/commit/2aa678f63eb0008cdf7f0b4147eee6a25b480796))

### Changed
- Restore chat history from persistent transcripts (chat_<conversationId>.json) on cache miss in ChatbotController ([99cca9d](https://github.com/id-data-foundry/data-foundry/commit/99cca9d4f05938275c5ca73df5e93eaeb7d2352f))
- Filter empty/orphan sessions from dropdown and automatically prune deleted sessions from sessions.json ([99cca9d](https://github.com/id-data-foundry/data-foundry/commit/99cca9d4f05938275c5ca73df5e93eaeb7d2352f))
- Redesign chat view header with clean PicoCSS navbar, auto-scrolling, and left-aligned user bubbles ([99cca9d](https://github.com/id-data-foundry/data-foundry/commit/99cca9d4f05938275c5ca73df5e93eaeb7d2352f))
- Align top bar action buttons (+ New Chat, Memory, Edit bot) and dropdown at uniform 32px height ([99cca9d](https://github.com/id-data-foundry/data-foundry/commit/99cca9d4f05938275c5ca73df5e93eaeb7d2352f))
- Replace generic "assistant" role label with bot name across chat view history and live HTMX responses ([99cca9d](https://github.com/id-data-foundry/data-foundry/commit/99cca9d4f05938275c5ca73df5e93eaeb7d2352f))
- Update documentation submodule for agentic chatbot features ([880d76a](https://github.com/id-data-foundry/data-foundry/commit/880d76a38db1b0505f385b9a8594c2af87afc14f))
- Generate multi-size favicon.ico and high-resolution apple-touch-icon assets ([c413c84](https://github.com/id-data-foundry/data-foundry/commit/c413c8465afd24ddeef06f275ad9b252d7052406))
- Update HTML layout templates to declare favicon and apple-touch-icon links ([c413c84](https://github.com/id-data-foundry/data-foundry/commit/c413c8465afd24ddeef06f275ad9b252d7052406))
- Refine ErrorHandler to eliminate duplicate logging, log 404s as WARN, and return 404 for static/API resources in prod mode ([c413c84](https://github.com/id-data-foundry/data-foundry/commit/c413c8465afd24ddeef06f275ad9b252d7052406))
- Title bar standardisation: ([4e71074](https://github.com/id-data-foundry/data-foundry/commit/4e710740126c523ad3d9b287cddcf6e08af029c0))
- Update chatbot view header to standard DataFoundry pattern with bot icon, inline h3 heading, floated action buttons, and aligned status chips ([4e71074](https://github.com/id-data-foundry/data-foundry/commit/4e710740126c523ad3d9b287cddcf6e08af029c0))
- Materialize UI styling alignment: ([4e71074](https://github.com/id-data-foundry/data-foundry/commit/4e710740126c523ad3d9b287cddcf6e08af029c0))
- Strip redundant inline borders/shadows from cards to leverage native Materialize card elevation ([4e71074](https://github.com/id-data-foundry/data-foundry/commit/4e710740126c523ad3d9b287cddcf6e08af029c0))
- Eliminate custom blues in favor of standard Materialize teal, green, and neutral palette ([4e71074](https://github.com/id-data-foundry/data-foundry/commit/4e710740126c523ad3d9b287cddcf6e08af029c0))
- Consolidate reusable styles for quick prompts, code textareas, and workflow cards ([4e71074](https://github.com/id-data-foundry/data-foundry/commit/4e710740126c523ad3d9b287cddcf6e08af029c0))
- Enclose test view prompt input and starter buttons inside a dedicated Materialize card ([4e71074](https://github.com/id-data-foundry/data-foundry/commit/4e710740126c523ad3d9b287cddcf6e08af029c0))
- Dynamic workflow diagram in test view: ([4e71074](https://github.com/id-data-foundry/data-foundry/commit/4e710740126c523ad3d9b287cddcf6e08af029c0))
- Adapt workflow diagram and explanatory cards to current bot configuration (Agentic vs. Standard, Memory enabled/disabled, KB documents present/absent) ([4e71074](https://github.com/id-data-foundry/data-foundry/commit/4e710740126c523ad3d9b287cddcf6e08af029c0))
- Conversations sorting & session management: ([4e71074](https://github.com/id-data-foundry/data-foundry/commit/4e710740126c523ad3d9b287cddcf6e08af029c0))
- Sort archived chat transcripts in Conversations tab descending by timestamp (newest first) ([4e71074](https://github.com/id-data-foundry/data-foundry/commit/4e710740126c523ad3d9b287cddcf6e08af029c0))
- Clarify 60-minute inactivity disclaimer between session cache eviction, archiving, and persistent memory ([4e71074](https://github.com/id-data-foundry/data-foundry/commit/4e710740126c523ad3d9b287cddcf6e08af029c0))
- Integrate inline modal link for memory management and generalize conversation reset confirmation phrasing ([4e71074](https://github.com/id-data-foundry/data-foundry/commit/4e710740126c523ad3d9b287cddcf6e08af029c0))
- Controller & styling refactoring: ([4e71074](https://github.com/id-data-foundry/data-foundry/commit/4e710740126c523ad3d9b287cddcf6e08af029c0))
- Update ChatbotController.testProcess to use CSS class chips rather than hardcoded inline styles ([4e71074](https://github.com/id-data-foundry/data-foundry/commit/4e710740126c523ad3d9b287cddcf6e08af029c0))
- Reorganize chatbot view into four tabs: Configuration, Knowledge Base, ([805ef3b](https://github.com/id-data-foundry/data-foundry/commit/805ef3b4fff896ff78696939a94f97352dca484e))
- Make Instructions (System Prompt) and RAG retrieval settings common to ([805ef3b](https://github.com/id-data-foundry/data-foundry/commit/805ef3b4fff896ff78696939a94f97352dca484e))
- Move chat transcript archiving switch to the General Information card. ([805ef3b](https://github.com/id-data-foundry/data-foundry/commit/805ef3b4fff896ff78696939a94f97352dca484e))
- Enforce strict Materialize grid standards (enclosing all .col elements in ([805ef3b](https://github.com/id-data-foundry/data-foundry/commit/805ef3b4fff896ff78696939a94f97352dca484e))
- Clean up UX: remove z-depth from tabs, standardize trash bin delete ([805ef3b](https://github.com/id-data-foundry/data-foundry/commit/805ef3b4fff896ff78696939a94f97352dca484e))
- Integrate HarnessAgent with KnowledgeSearchTool and UserMemoryTool for autonomous RAG and persistent notes ([8194302](https://github.com/id-data-foundry/data-foundry/commit/8194302ce24c5dee230013c40d7abb56b66674be))
- Implement automatic resumption turns and trace-aware fallback engine to eliminate empty agent outputs ([8194302](https://github.com/id-data-foundry/data-foundry/commit/8194302ce24c5dee230013c40d7abb56b66674be))
- Redesign test view layout with unified .conversation-row flex rules and aligned 140px role labels ([8194302](https://github.com/id-data-foundry/data-foundry/commit/8194302ce24c5dee230013c40d7abb56b66674be))
- Attach statementLimit with sourceFilter to exclude internal runtime and js-beautify library ([7f2bca7](https://github.com/id-data-foundry/data-foundry/commit/7f2bca76786a4c971ea792f5fb6dd289c73e8a1c))
- Translate PolyglotException resource exhaustion into ScriptCPUAbuseException ([7f2bca7](https://github.com/id-data-foundry/data-foundry/commit/7f2bca76786a4c971ea792f5fb6dd289c73e8a1c))
- Emit durable audit trail in LabNotesEntry with SHA-256 hash on save, install, and execute ([cecf31c](https://github.com/id-data-foundry/data-foundry/commit/cecf31c19441cd559cfc885aedd29edb27fbf841))
- Strip CR and LF characters from notification title and tags before sending HTTP request ([3431830](https://github.com/id-data-foundry/data-foundry/commit/34318307383f1977b9c37a66c1a29bb9e9676589))
- Eliminate blocking do...while cache polling loop on single-thread executor ([8015fab](https://github.com/id-data-foundry/data-foundry/commit/8015fab56f00430e9080c528efc73c3b37e17e54))
- Directly return asynchronously transcribed output from scheduleMediaToTextProcess ([8015fab](https://github.com/id-data-foundry/data-foundry/commit/8015fab56f00430e9080c528efc73c3b37e17e54))
- Align cache key prefix to DatasetsController_httpPostDiagnostics_ in ActivityLoggerController ([356198e](https://github.com/id-data-foundry/data-foundry/commit/356198e5237f996767aa953e28ec599ea2c67e66))
- Replace non-constant-time token and secret comparisons across controllers and models ([e857231](https://github.com/id-data-foundry/data-foundry/commit/e857231bb193329c23de0108066a53dda6401b01))
- Inject DatabaseExecutionContext into ProjectsController and ChatbotController ([7f9c5ab](https://github.com/id-data-foundry/data-foundry/commit/7f9c5ab5ebf18782ae92ccb44daadd8a9160a592))
- Prevent database thread starvation on ForkJoinPool.commonPool() ([7f9c5ab](https://github.com/id-data-foundry/data-foundry/commit/7f9c5ab5ebf18782ae92ccb44daadd8a9160a592))
- Make agent, subAgent, and agentsMdLastModified context fields volatile ([230f5a5](https://github.com/id-data-foundry/data-foundry/commit/230f5a547dbefb552e9ba3783a47fd9f4cbd4ebb))
- Move agent initialization outside map compute lock to prevent thread contention ([230f5a5](https://github.com/id-data-foundry/data-foundry/commit/230f5a547dbefb552e9ba3783a47fd9f4cbd4ebb))
- Track active WebSocket connections and evict dataset context upon termination ([230f5a5](https://github.com/id-data-foundry/data-foundry/commit/230f5a547dbefb552e9ba3783a47fd9f4cbd4ebb))
- Call getFileInternal instead of getFile in readFileContentFromDisk to prevent recursive Caffeine cache deadlock ([dbb8c42](https://github.com/id-data-foundry/data-foundry/commit/dbb8c42d36a9ea1c3a2b0cf15b7d76b4d95085d6))
- Run container as non-root system user datafoundry:datafoundry (UID/GID 10001) ([de99102](https://github.com/id-data-foundry/data-foundry/commit/de99102cec34d484fdbc4385d14f1b2e1b19185c))
- First draft agentic chatbot ([ee40815](https://github.com/id-data-foundry/data-foundry/commit/ee408153b459c726d1b540397eb43b638c83e091))

### Removed
- Remove individual user memory inspector from administration view. ([805ef3b](https://github.com/id-data-foundry/data-foundry/commit/805ef3b4fff896ff78696939a94f97352dca484e))
- Remove dead publishingStatus_ fallback check in DatasetsController.downloadTemporaryFile ([356198e](https://github.com/id-data-foundry/data-foundry/commit/356198e5237f996767aa953e28ec599ea2c67e66))
- Remove unused, unsigned lib/swagger-java-client-1.0.0.jar from repo and Dockerfile ([de99102](https://github.com/id-data-foundry/data-foundry/commit/de99102cec34d484fdbc4385d14f1b2e1b19185c))

### Fixed
- Fix memory dialog file accordion to place expand chevron at start and dock delete button at far right ([99cca9d](https://github.com/id-data-foundry/data-foundry/commit/99cca9d4f05938275c5ca73df5e93eaeb7d2352f))
- Resolve favicon and apple-touch-icon routing and ErrorHandler client error logs ([c413c84](https://github.com/id-data-foundry/data-foundry/commit/c413c8465afd24ddeef06f275ad9b252d7052406))
- Prevent HTMX body swap on save/install/deinstall and return HTTP 204 ([9b496c6](https://github.com/id-data-foundry/data-foundry/commit/9b496c6e5392ae2e7f0d88fd483312601e20e670))
- Strip 'server' parameter in preProcessRequest and normalize base URL in local-ai.js ([bc2632f](https://github.com/id-data-foundry/data-foundry/commit/bc2632fc52e6f071996a84945821d4fece5aeb0c))
- Strip internal api_token and task parameters before forwarding to upstream LLM ([8fb7689](https://github.com/id-data-foundry/data-foundry/commit/8fb768980f96a0c24a55c3d464719f7b8ce1a09e))
- Sync public content and provide fallback fixtures for CI/CD test runs ([da20dd2](https://github.com/id-data-foundry/data-foundry/commit/da20dd275ca9d9242bb899d93e3b05b7295326bb))
- Provide fallback documentation test fixtures when Jekyll artifacts are absent ([14b40b3](https://github.com/id-data-foundry/data-foundry/commit/14b40b329fce5fa05b64b33d6bed8c721ed91212))
- Configure statement limits, source filtering, and limit reset in GraalSandboxImpl (DF-79) ([7f2bca7](https://github.com/id-data-foundry/data-foundry/commit/7f2bca76786a4c971ea792f5fb6dd289c73e8a1c))
- Enforce CSRF, audit script actions in LabNotesEntry, and migrate UI to HTMX (DF-78) ([cecf31c](https://github.com/id-data-foundry/data-foundry/commit/cecf31c19441cd559cfc885aedd29edb27fbf841))
- Sanitize notification title and tags against CRLF injection (DF-76) ([3431830](https://github.com/id-data-foundry/data-foundry/commit/34318307383f1977b9c37a66c1a29bb9e9676589))
- Remove unbounded cache polling loop in AudioProcessingApiService (DF-67) ([8015fab](https://github.com/id-data-foundry/data-foundry/commit/8015fab56f00430e9080c528efc73c3b37e17e54))
- Align ActivityLoggerController cache key and remove dead fallback (DF-66) ([356198e](https://github.com/id-data-foundry/data-foundry/commit/356198e5237f996767aa953e28ec599ea2c67e66))
- Route async database operations to dedicated DatabaseExecutionContext (DF-35) ([7f9c5ab](https://github.com/id-data-foundry/data-foundry/commit/7f9c5ab5ebf18782ae92ccb44daadd8a9160a592))
- Ensure thread safety and connection lifecycle cleanup in CodingAgentController (DF-32) ([230f5a5](https://github.com/id-data-foundry/data-foundry/commit/230f5a547dbefb552e9ba3783a47fd9f4cbd4ebb))
- Eliminate recursive cache re-entrancy in CompleteDS file retrieval (DF-33) ([dbb8c42](https://github.com/id-data-foundry/data-foundry/commit/dbb8c42d36a9ea1c3a2b0cf15b7d76b4d95085d6))
- Remove unsigned swagger jar and run container as non-root (DF-26, DF-56) ([de99102](https://github.com/id-data-foundry/data-foundry/commit/de99102cec34d484fdbc4385d14f1b2e1b19185c))
- Fixed PDF dependencies ([90cbeb4](https://github.com/id-data-foundry/data-foundry/commit/90cbeb4f22fe88dda02d04e6196b389c3c8abb00))

## [v0.9.33] - 2026-10-01
### Security
- Preserve DF-11 invite token security fix on form record endpoint ([63593cb](https://github.com/id-data-foundry/data-foundry/commit/63593cb4acdb27ccf02171284c09f6100d4902ae))
- Validate dataset config, scope telegram replies, share credit locks, and redact sensitive logs (DF-28, DF-29, DF-30, DF-65, DF-71, DF-72) ([7848157](https://github.com/id-data-foundry/data-foundry/commit/784815791f8060f1129fb606713dd2a8d3c135f3))
- Upgrade legacy password hashing and enforce constant-time webhook verification (DF-22, DF-38, DF-63, DF-77) ([01b7379](https://github.com/id-data-foundry/data-foundry/commit/01b737988dc6ead00107e562c469a2e0021d545b))
- Sanitize HTML rendering, attribute interpolation, and template injection (DF-23, DF-24, DF-45, DF-46, DF-47) ([cb3f292](https://github.com/id-data-foundry/data-foundry/commit/cb3f292e30eb3082c4e3d02d30357d4b87707710))
- Restrict setItem in UserApiController to project editors or enrolled participants (DF-62) ([bb1f98f](https://github.com/id-data-foundry/data-foundry/commit/bb1f98fa17db18de833d479d6ad8a91bc45b1a79))
- Add UserApiControllerSecurityTest verifying strangers are blocked on public ([bb1f98f](https://github.com/id-data-foundry/data-foundry/commit/bb1f98fa17db18de833d479d6ad8a91bc45b1a79))
- Harden chatbot access, scope cache keys, and synchronize history (DF-12, DF-20, DF-34) ([5401f79](https://github.com/id-data-foundry/data-foundry/commit/5401f7928cb873c5de1381c6d9ad7ec36e7bd0ba))
- Add ChatbotControllerSecurityTest verifying access controls, cache scoping, and concurrency safety. ([5401f79](https://github.com/id-data-foundry/data-foundry/commit/5401f7928cb873c5de1381c6d9ad7ec36e7bd0ba))
- Namespace QRCode cache keys to prevent cache poisoning (DF-19) ([1ad730f](https://github.com/id-data-foundry/data-foundry/commit/1ad730ffbc7875fcee32747c3797c93f9a74c4cf))
- Add QRCodeSecurityTest verifying cache key namespacing ([1ad730f](https://github.com/id-data-foundry/data-foundry/commit/1ad730ffbc7875fcee32747c3797c93f9a74c4cf))
- Harden GPX parser against UTF-16 XXE, sanitize CSV exports and filenames, neutralize markdown link XSS (DF-16, DF-44, DF-64, DF-70) ([32c0aec](https://github.com/id-data-foundry/data-foundry/commit/32c0aecab0702d6bf1bef66f879a69ed238f4f1f))
- Sanitizes dynamic strings and projection values with cf() across all dataset CSV export loops (TimeseriesDS, FormDS, SurveyDS, ExpSamplingDS, EntityDS, MovementDS) to prevent spreadsheet formula injection (DF-44) ([32c0aec](https://github.com/id-data-foundry/data-foundry/commit/32c0aecab0702d6bf1bef66f879a69ed238f4f1f))
- Sanitizes and quotes Content-Disposition filenames via DataUtils.sanitizeFilenameForContentDisposition in ApiSpecs and DatasetApiController (DF-64) ([32c0aec](https://github.com/id-data-foundry/data-foundry/commit/32c0aecab0702d6bf1bef66f879a69ed238f4f1f))
- Gate addIoTRecord with V2UserApiAuth and harden DatasetApiAuth path validation (DF-10, DF-42) ([61731f9](https://github.com/id-data-foundry/data-foundry/commit/61731f9a82c72b3d6b7840e251e251c2e1602740))
- Deterministic stable tokens, hash injection guard, and reset token expiration (DF-04, DF-05, DF-37, DF-39) ([8af4413](https://github.com/id-data-foundry/data-foundry/commit/8af441341a2ec2e5b1fed3bb7c062bfbdacd4b96))
- Eliminate SQL LIKE wildcard vulnerabilities and harden endpoint visibility and access controls (DF-48, DF-54, DF-60, DF-61, DF-69) ([f99dc5a](https://github.com/id-data-foundry/data-foundry/commit/f99dc5ad1ecbe32562b7ea4e484d800ef808c76f))
- Mitigate Host header poisoning, enforce CSP on SVG assets, and support multi-module dist paths (DF-08, DF-09, DF-15) ([e4d8c13](https://github.com/id-data-foundry/data-foundry/commit/e4d8c13077f974a52717e9f0bb14ad7205f678c5))
- Restrict AI documentation API to authenticated users, bound throttling cache, and namespace image keys (DF-03, DF-31, DF-53) ([4f2c695](https://github.com/id-data-foundry/data-foundry/commit/4f2c6950f82c2418fcdb65652c4b3ea54b20bcab))
- Require invite token for form dataset record submissions and namespace guard cache keys (DF-11) ([f5f9318](https://github.com/id-data-foundry/data-foundry/commit/f5f9318a58c2ba22f1309bf5dffbf9b4f002676c))
- Harden project token resolution with secret key fallback and reject blank registration keys (DF-06, DF-41) ([5ac90d7](https://github.com/id-data-foundry/data-foundry/commit/5ac90d75124f96038cb1838a941cb78e5405fa2d))
- Sanitize auth key placeholder and remove exposed debug/db ports in compose templates (DF-01, DF-18) ([7d75548](https://github.com/id-data-foundry/data-foundry/commit/7d7554872e60063a7600473977b6985ca3250f91))
- Add -Xss16m to H2 server startup in h2.service to accommodate deep ([e5625a9](https://github.com/id-data-foundry/data-foundry/commit/e5625a9446134cff4178aaf3099d75a4e71b3743))
- Gate coding agent on editableBy instead of visibleFor ([02a4464](https://github.com/id-data-foundry/data-foundry/commit/02a446477e986e337af53acf8813097ba60e78f2))
- Add `CodingAgentSecurityTest` validating access across owners, collaborators, ([02a4464](https://github.com/id-data-foundry/data-foundry/commit/02a446477e986e337af53acf8813097ba60e78f2))
- Preserve Security Finding H5 protection: external domains and URL ([3f502a9](https://github.com/id-data-foundry/data-foundry/commit/3f502a900be3d72cc1b524432a09d28afb5010dd))

### Added
- Add diagnostic warning in refresh() when upstream returns 401 ([06aedeb](https://github.com/id-data-foundry/data-foundry/commit/06aedeb39d1daae90b74c16abe16fc6fc83c11a2))
- Add UnmanagedAIApiServiceAuthTest unit test suite ([06aedeb](https://github.com/id-data-foundry/data-foundry/commit/06aedeb39d1daae90b74c16abe16fc6fc83c11a2))
- Add graceful fallbacks to multi-part first names and email initial when ([506570f](https://github.com/id-data-foundry/data-foundry/commit/506570f48777562747bb78cec51aa445c4d5f92a))
- Add comprehensive JUnit test suite PersonInitialsTest covering 8 edge-case ([506570f](https://github.com/id-data-foundry/data-foundry/commit/506570f48777562747bb78cec51aa445c4d5f92a))
- Add Restart=on-failure and RestartSec=5s to h2.service ([e5625a9](https://github.com/id-data-foundry/data-foundry/commit/e5625a9446134cff4178aaf3099d75a4e71b3743))
- Add comprehensive JUnit test suite covering development, production WAF, ([3f502a9](https://github.com/id-data-foundry/data-foundry/commit/3f502a900be3d72cc1b524432a09d28afb5010dd))
- Added duplicate feature to script dataset ([c8bc49f](https://github.com/id-data-foundry/data-foundry/commit/c8bc49f713bb2f262d502aadd4e701f52d56f761))
- Added duplicatioin feature to datasets ([2d523f7](https://github.com/id-data-foundry/data-foundry/commit/2d523f7a5d0573df2ee47aee660437b2644f2145))
- Added DUPLICATE button to edit page of datasets ([2d523f7](https://github.com/id-data-foundry/data-foundry/commit/2d523f7a5d0573df2ee47aee660437b2644f2145))
- Added duplicate form for datasets ([2d523f7](https://github.com/id-data-foundry/data-foundry/commit/2d523f7a5d0573df2ee47aee660437b2644f2145))

### Changed
- Resolve conflicts in CompleteDSController and common.routes ([63593cb](https://github.com/id-data-foundry/data-foundry/commit/63593cb4acdb27ccf02171284c09f6100d4902ae))
- Enforce project.editableBy permissions check directly in DatasetsController.duplicate ([63593cb](https://github.com/id-data-foundry/data-foundry/commit/63593cb4acdb27ccf02171284c09f6100d4902ae))
- Centralize outbound HTTP request creation via prepareWSRequest helper ([06aedeb](https://github.com/id-data-foundry/data-foundry/commit/06aedeb39d1daae90b74c16abe16fc6fc83c11a2))
- Normalize Bearer prefix to avoid duplicate prefixes ([06aedeb](https://github.com/id-data-foundry/data-foundry/commit/06aedeb39d1daae90b74c16abe16fc6fc83c11a2))
- Propagate API key to prepareWSRemoteAPIRequest, doOpenStream, pingEndpoint, ([06aedeb](https://github.com/id-data-foundry/data-foundry/commit/06aedeb39d1daae90b74c16abe16fc6fc83c11a2))
- Reject HTTP 401 and 403 in pingEndpoint availability checks ([06aedeb](https://github.com/id-data-foundry/data-foundry/commit/06aedeb39d1daae90b74c16abe16fc6fc83c11a2))
- Properly initialize internalAPIRequest with SYSTEM user in refresh() ([06aedeb](https://github.com/id-data-foundry/data-foundry/commit/06aedeb39d1daae90b74c16abe16fc6fc83c11a2))
- Restrict UserApiController.setItem to require project.editableBy(user) ([bb1f98f](https://github.com/id-data-foundry/data-foundry/commit/bb1f98fa17db18de833d479d6ad8a91bc45b1a79))
- Return 403 Forbidden when an unauthorized stranger attempts to write. ([bb1f98f](https://github.com/id-data-foundry/data-foundry/commit/bb1f98fa17db18de833d479d6ad8a91bc45b1a79))
- UploadFileApi accepted study participant invite tokens and lacked dataset type checks, ([5401f79](https://github.com/id-data-foundry/data-foundry/commit/5401f7928cb873c5de1381c6d9ad7ec36e7bd0ba))
- ChatApi did not enforce that the target dataset was a configured CompleteDS chatbot (DF-12). ([5401f79](https://github.com/id-data-foundry/data-foundry/commit/5401f7928cb873c5de1381c6d9ad7ec36e7bd0ba))
- Chatbot conversation cache keys were only keyed by conversationId, allowing cross-dataset ([5401f79](https://github.com/id-data-foundry/data-foundry/commit/5401f7928cb873c5de1381c6d9ad7ec36e7bd0ba))
- ConversationHistory.items used LinkedList without synchronization, leading to race ([5401f79](https://github.com/id-data-foundry/data-foundry/commit/5401f7928cb873c5de1381c6d9ad7ec36e7bd0ba))
- Restrict uploadFileApi to project editors (editableBy) or dataset API token holders, ([5401f79](https://github.com/id-data-foundry/data-foundry/commit/5401f7928cb873c5de1381c6d9ad7ec36e7bd0ba))
- Verify in chatApi that target dataset is a configured COMPLETE chatbot (CHATBOT_MODEL set). ([5401f79](https://github.com/id-data-foundry/data-foundry/commit/5401f7928cb873c5de1381c6d9ad7ec36e7bd0ba))
- Scope chatbot cache keys by dataset ID via getChatCacheKey: ChatController_chat_<dsId>_<conversationId>. ([5401f79](https://github.com/id-data-foundry/data-foundry/commit/5401f7928cb873c5de1381c6d9ad7ec36e7bd0ba))
- Make ConversationHistory thread-safe with CopyOnWriteArrayList and synchronize prompt/response updates. ([5401f79](https://github.com/id-data-foundry/data-foundry/commit/5401f7928cb873c5de1381c6d9ad7ec36e7bd0ba))
- Prefix all QRCode cache keys with cached_qrcode_ ([1ad730f](https://github.com/id-data-foundry/data-foundry/commit/1ad730ffbc7875fcee32747c3797c93f9a74c4cf))
- When key is null or shorter than 6 characters, compute a SHA-256 hash of the ([1ad730f](https://github.com/id-data-foundry/data-foundry/commit/1ad730ffbc7875fcee32747c3797c93f9a74c4cf))
- Normalizes zero-bytes in MovementDS.containsXmlEntityOrDocType to detect DOCTYPE and ENTITY declarations across UTF-8, UTF-16LE, UTF-16BE, and UTF-32 (DF-16) ([32c0aec](https://github.com/id-data-foundry/data-foundry/commit/32c0aecab0702d6bf1bef66f879a69ed238f4f1f))
- Configures SafeUrlAttributeProvider in MarkdownRenderer to neutralize javascript:, vbscript:, and data: links and images (DF-70) ([32c0aec](https://github.com/id-data-foundry/data-foundry/commit/32c0aecab0702d6bf1bef66f879a69ed238f4f1f))
- Adds unit tests for UTF-16 GPX XXE detection, Content-Disposition filename sanitization, and Markdown URL neutralization ([32c0aec](https://github.com/id-data-foundry/data-foundry/commit/32c0aecab0702d6bf1bef66f879a69ed238f4f1f))
- Adds missing @Authenticated(V2UserApiAuth.class) to DatasetApiController.addIoTRecord (DF-10) ([61731f9](https://github.com/id-data-foundry/data-foundry/commit/61731f9a82c72b3d6b7840e251e251c2e1602740))
- Hardens DatasetApiAuth to parse dataset ID from arbitrary /datasets/ and /api/v1/datasets/ routes and verify path ID matches authorized dataset (DF-42) ([61731f9](https://github.com/id-data-foundry/data-foundry/commit/61731f9a82c72b3d6b7840e251e251c2e1602740))
- Upgrades dataset API token comparison to timing-safe MessageDigest.isEqual ([61731f9](https://github.com/id-data-foundry/data-foundry/commit/61731f9a82c72b3d6b7840e251e251c2e1602740))
- Derives deterministic 12-byte IV for SymEncryption.getStableToken via SHA-256(secKey + ":" + plaintext), ensuring stable tokens remain consistent across calls and reboots for datastore lookups (DF-04) ([8af4413](https://github.com/id-data-foundry/data-foundry/commit/8af441341a2ec2e5b1fed3bb7c062bfbdacd4b96))
- Adds deprecation warning when decrypting legacy AES-ECB tokens (DF-05) ([8af4413](https://github.com/id-data-foundry/data-foundry/commit/8af441341a2ec2e5b1fed3bb7c062bfbdacd4b96))
- Removes isHashed bypass in Hash.hashPassword to always hash with BCrypt, preventing password hash injection and unsalted SHA-512 downgrading (DF-37) ([8af4413](https://github.com/id-data-foundry/data-foundry/commit/8af441341a2ec2e5b1fed3bb7c062bfbdacd4b96))
- Embeds 2-hour expiration timestamp and nonce into TokenResolverUtil.createEmailResetToken and enforces freshness checks in retrieveUsernameFromEmailResetToken (DF-39) ([8af4413](https://github.com/id-data-foundry/data-foundry/commit/8af441341a2ec2e5b1fed3bb7c062bfbdacd4b96))
- Adds test cases for stable token determinism, password downgrade prevention, and expired reset tokens ([8af4413](https://github.com/id-data-foundry/data-foundry/commit/8af441341a2ec2e5b1fed3bb7c062bfbdacd4b96))
- Make Person.getInitials() resilient against null, empty, whitespace-only, ([506570f](https://github.com/id-data-foundry/data-foundry/commit/506570f48777562747bb78cec51aa445c4d5f92a))
- Pre-sort LASTNAME_PREFIXES array statically descending by length once ([506570f](https://github.com/id-data-foundry/data-foundry/commit/506570f48777562747bb78cec51aa445c4d5f92a))
- Resolves HTTP 500 crashes during authenticated user resolution in ([506570f](https://github.com/id-data-foundry/data-foundry/commit/506570f48777562747bb78cec51aa445c4d5f92a))
- Configure HikariCP connectionTestQuery, validationTimeout (3s), and ([e5625a9](https://github.com/id-data-foundry/data-foundry/commit/e5625a9446134cff4178aaf3099d75a4e71b3743))
- Restrict `view(request, id, fileId)` to `ds.editableBy(username)` ([02a4464](https://github.com/id-data-foundry/data-foundry/commit/02a446477e986e337af53acf8813097ba60e78f2))
- Restrict `getFileList(request, id)` to `ds.editableBy(username)` ([02a4464](https://github.com/id-data-foundry/data-foundry/commit/02a446477e986e337af53acf8813097ba60e78f2))
- Restrict WebSocket `ws(id)` handshake to `ds.editableBy(user)` ([02a4464](https://github.com/id-data-foundry/data-foundry/commit/02a446477e986e337af53acf8813097ba60e78f2))
- Validate origin by checking parsed hostname (refUrl.getHost()) against ([3f502a9](https://github.com/id-data-foundry/data-foundry/commit/3f502a900be3d72cc1b524432a09d28afb5010dd))
- Allow documentation requests in authorize() that omit an Authorization ([3f502a9](https://github.com/id-data-foundry/data-foundry/commit/3f502a900be3d72cc1b524432a09d28afb5010dd))
- Set Referrer-Policy: same-origin on HTML documentation responses in ([3f502a9](https://github.com/id-data-foundry/data-foundry/commit/3f502a900be3d72cc1b524432a09d28afb5010dd))

### Removed
- Remove 10 redundant POST duplicate routes; add GET /datasets/duplicate/:id alias ([63593cb](https://github.com/id-data-foundry/data-foundry/commit/63593cb4acdb27ccf02171284c09f6100d4902ae))

### Fixed
- Fix ActorController to support both dataset_name and name fields and fix String equality check ([63593cb](https://github.com/id-data-foundry/data-foundry/commit/63593cb4acdb27ccf02171284c09f6100d4902ae))
- Propagate configured API key to models call and outbound AI requests ([06aedeb](https://github.com/id-data-foundry/data-foundry/commit/06aedeb39d1daae90b74c16abe16fc6fc83c11a2))
- Prevent StringIndexOutOfBoundsException in Person.getInitials() ([506570f](https://github.com/id-data-foundry/data-foundry/commit/506570f48777562747bb78cec51aa445c4d5f92a))
- Prevent H2 MVStore StackOverflowError and improve pool resilience ([e5625a9](https://github.com/id-data-foundry/data-foundry/commit/e5625a9446134cff4178aaf3099d75a4e71b3743))
- Resolve documentation API key exception behind reverse proxy and WAF ([3f502a9](https://github.com/id-data-foundry/data-foundry/commit/3f502a900be3d72cc1b524432a09d28afb5010dd))

## [v0.9.32] - 2026-09-21
### Security
- Make AbstractAsyncController.csrfToken() extraction safe against missing CSRF tokens ([723a5e5](https://github.com/id-data-foundry/data-foundry/commit/723a5e5ef13bc6be690259382f5b5524b7df6a8e))
- Unify dist file and folder retrieval across HomeController and ProjectsController supporting templates and symlinks with strict security ([836b277](https://github.com/id-data-foundry/data-foundry/commit/836b277c4012208cfb5b77569d38d43bb376a9a8))
- Harden GraalVM sandbox against class lookup/polyglot escapes and exclude jackson from azure/microsoft deps ([5e69986](https://github.com/id-data-foundry/data-foundry/commit/5e69986a8761527018cc8effe17e187308f6dca3))
- Harden data handling, CSV formula injection defense, and sandbox security (Component 6) ([eab42f5](https://github.com/id-data-foundry/data-foundry/commit/eab42f550fcc8851a040e16c345295599b6ee115))
- Implement CSV formula injection defense in DataUtils.sanitizeCsvCell escaping quotes and prepending apostrophe to =, @, tab, cr, and non-numeric +, - ([eab42f5](https://github.com/id-data-foundry/data-foundry/commit/eab42f550fcc8851a040e16c345295599b6ee115))
- Route CSV export cell formatting across LinkedDS, AnnotationDS, DiaryDS, FormDS, SurveyDS, and StringUtils through DataUtils.sanitizeCsvCell ([eab42f5](https://github.com/id-data-foundry/data-foundry/commit/eab42f550fcc8851a040e16c345295599b6ee115))
- Restrict GraalVM Polyglot context in GraalSandboxImpl to HostAccess.EXPLICIT to prevent host class escapes ([eab42f5](https://github.com/id-data-foundry/data-foundry/commit/eab42f550fcc8851a040e16c345295599b6ee115))
- Add comprehensive unit tests in DataUtilsCsvSanitizeTest and GraalSandboxSecurityTest ([eab42f5](https://github.com/id-data-foundry/data-foundry/commit/eab42f550fcc8851a040e16c345295599b6ee115))
- Sanitize outputs, escape raw HTML, and prevent script breakout ([7458afe](https://github.com/id-data-foundry/data-foundry/commit/7458afe9dcb690733b13c99eaae6cabb634a20fe))
- Enforce API authorization, tenant isolation, and IDOR protections ([dc0101e](https://github.com/id-data-foundry/data-foundry/commit/dc0101ee6cff4b6d32220611b62f2f2aae2f87e7))
- Prevent path traversal and XML entity injection in file storage and imports ([efbdb05](https://github.com/id-data-foundry/data-foundry/commit/efbdb057c3688c01d6246aad1d5ff3e14f3e1b46))
- Sanitize redirect URLs and sandbox user web dataset responses ([86afc5e](https://github.com/id-data-foundry/data-foundry/commit/86afc5ed0568cee7feef8de149d494999e21a896))
- Backward-compatible v2 AES-GCM tokens, BCrypt hashing, and timing-safe auth ([b7ea8e0](https://github.com/id-data-foundry/data-foundry/commit/b7ea8e05b59ac135c7ad1f9453c4c924de5f1a08))

### Added
- Add CompleteDSViewTest verifying view rendering and cache behavior ([723a5e5](https://github.com/id-data-foundry/data-foundry/commit/723a5e5ef13bc6be690259382f5b5524b7df6a8e))
- Add fallback UI ("click here to download" and "Back to project" links) in publish response when automatic browser download is blocked ([38f87be](https://github.com/id-data-foundry/data-foundry/commit/38f87bead19e60cde2487790ddca79f678628a90))
- Add end-to-end integration tests in ProjectExportTest covering successful export archive extraction, invalid token handling, and missing file handling ([38f87be](https://github.com/id-data-foundry/data-foundry/commit/38f87bead19e60cde2487790ddca79f678628a90))
- Add in-memory caching to CompleteDS with SyncCacheApi for file metadata and content, serving reads with zero DB queries ([3a1f45b](https://github.com/id-data-foundry/data-foundry/commit/3a1f45be485e2937390cac7ef11964b2bc5d9e42))
- Add index on file_name in ds_*_cp tables to accelerate queries ([3a1f45b](https://github.com/id-data-foundry/data-foundry/commit/3a1f45be485e2937390cac7ef11964b2bc5d9e42))

### Changed
- Bump version to 0.9.32-SNAPSHOT ([f96f894](https://github.com/id-data-foundry/data-foundry/commit/f96f894ccb392baa1305f4de1c8bc3c301d20eb4))
- Verify DF.* print, logging, data access, telegram, and oocsi method invocations are unblocked ([928568b](https://github.com/id-data-foundry/data-foundry/commit/928568b72e362ace7e8200122a28cf270ee6ce4c))
- Add URL transmission safety verification for all generated token types ([b05eafe](https://github.com/id-data-foundry/data-foundry/commit/b05eafe13ec41b2fa7ee45b8a82c8ab1921b5568))
- Add comprehensive backwards compatibility verification for all legacy tokens ([1b4b7e5](https://github.com/id-data-foundry/data-foundry/commit/1b4b7e5738612a8fb07d42efe5395a53520c22a7))
- Code formatting, code style alignment ([50185d7](https://github.com/id-data-foundry/data-foundry/commit/50185d743b2d7367ab2ef7a9302033fb56410348))
- Extend export token cache TTL from 30 seconds to 1200 seconds (20 minutes) to align with server file retention ([38f87be](https://github.com/id-data-foundry/data-foundry/commit/38f87bead19e60cde2487790ddca79f678628a90))
- Update DatasetsController.downloadTemporaryFile to verify physical file existence and return 404 Not Found on missing/expired tokens instead of silent 204 No Content ([38f87be](https://github.com/id-data-foundry/data-foundry/commit/38f87bead19e60cde2487790ddca79f678628a90))
- Set Content-Disposition: attachment and MIME type application/zip with project-specific filename in download response ([38f87be](https://github.com/id-data-foundry/data-foundry/commit/38f87bead19e60cde2487790ddca79f678628a90))
- Guard AssetsHelper.zipFile against missing disk files and ensure ZipEntry instances are closed properly ([38f87be](https://github.com/id-data-foundry/data-foundry/commit/38f87bead19e60cde2487790ddca79f678628a90))
- Clean up intermediate dataset JSON/CSV temp files immediately after zipping in ProjectLifecycleService ([38f87be](https://github.com/id-data-foundry/data-foundry/commit/38f87bead19e60cde2487790ddca79f678628a90))
- Eliminate explicit DB.beginTransaction() on CompleteDS read queries, closing auto-commit connections immediately before disk I/O ([3a1f45b](https://github.com/id-data-foundry/data-foundry/commit/3a1f45be485e2937390cac7ef11964b2bc5d9e42))
- Isolate CodingAgentController completions on a dedicated work-stealing thread pool (Executors.newWorkStealingPool()) ([3a1f45b](https://github.com/id-data-foundry/data-foundry/commit/3a1f45be485e2937390cac7ef11964b2bc5d9e42))
- Reduce synchronization scope from CodingAgentController singleton to per-dataset context and use ConcurrentHashMap ([3a1f45b](https://github.com/id-data-foundry/data-foundry/commit/3a1f45be485e2937390cac7ef11964b2bc5d9e42))
- Direct internal agent LLM loopback calls to localhost using play.server.http.port instead of routing through external df.base_url ([3a1f45b](https://github.com/id-data-foundry/data-foundry/commit/3a1f45be485e2937390cac7ef11964b2bc5d9e42))
- Eliminate client-side HTTP request stampedes on WebSocket file-sync events by updating dropdown DOM directly in view.scala.html ([3a1f45b](https://github.com/id-data-foundry/data-foundry/commit/3a1f45be485e2937390cac7ef11964b2bc5d9e42))
- Update CodingAgentUtilsTest for localhost port resolution (all 8 tests passing) ([3a1f45b](https://github.com/id-data-foundry/data-foundry/commit/3a1f45be485e2937390cac7ef11964b2bc5d9e42))
- Use SecureRandom for Telegram personal and project PIN generation in TelegramBotUtils ([eab42f5](https://github.com/id-data-foundry/data-foundry/commit/eab42f550fcc8851a040e16c345295599b6ee115))
- Eliminate blocking get() in MediaToTextController.processPDF to avoid thread starvation ([eab42f5](https://github.com/id-data-foundry/data-foundry/commit/eab42f550fcc8851a040e16c345295599b6ee115))
- Modernize commons-io, azure-identity, add jbcrypt, scope h2 to test ([16bc437](https://github.com/id-data-foundry/data-foundry/commit/16bc4371bf1cafa6be3002533d5e74b05a80268e))

### Removed
- Remove redundant outer cache.getOrElseUpdate() calls in CompleteDSController (view, editFile) and CodingAgentController (view, getFileList) that caused a re-entrant cache deadlock with CompleteDS.getFiles() ([723a5e5](https://github.com/id-data-foundry/data-foundry/commit/723a5e5ef13bc6be690259382f5b5524b7df6a8e))

### Fixed
- Serve web assets with explicit MIME types and enable same-origin subresources in CSP sandbox ([e625409](https://github.com/id-data-foundry/data-foundry/commit/e625409bc3f307bf4886573e91f0363567818c80))
- Resolve recursive cache deadlock on CompleteDS view and edit loading ([723a5e5](https://github.com/id-data-foundry/data-foundry/commit/723a5e5ef13bc6be690259382f5b5524b7df6a8e))
- Fixed link to TU/e scientific integrity page ([b2d7144](https://github.com/id-data-foundry/data-foundry/commit/b2d7144926798fab192b68ed4562731964e9b259))
- Resolve project zip export cache key mismatch and failure handling ([38f87be](https://github.com/id-data-foundry/data-foundry/commit/38f87bead19e60cde2487790ddca79f678628a90))
- Fix cache key discrepancy where ProjectsController cached temporary zip paths under publishingStatus_ instead of cachedTemporaryFile_ expected by DatasetsController ([38f87be](https://github.com/id-data-foundry/data-foundry/commit/38f87bead19e60cde2487790ddca79f678628a90))
- Resolve concurrency bottlenecks, thread starvation, and CompleteDS connection exhaustion ([3a1f45b](https://github.com/id-data-foundry/data-foundry/commit/3a1f45be485e2937390cac7ef11964b2bc5d9e42))

## [v0.9.31] - 2026-09-21
### Security
- Broaden CompleteDSController test suite to cover all download actions ([6f399b0](https://github.com/id-data-foundry/data-foundry/commit/6f399b022d98840fee27d7cc31213296bf9b93de))
- Prevent path traversal and harden public file downloads in CompleteDS ([9ff735b](https://github.com/id-data-foundry/data-foundry/commit/9ff735bcafc0ce1bac25712f4472781952d3f11e))

### Added
- Add automated integration tests in CompleteDSControllerPublicDownloadTest ([9ff735b](https://github.com/id-data-foundry/data-foundry/commit/9ff735bcafc0ce1bac25712f4472781952d3f11e))
- Unblock AI proxy with per-category lanes, direct streaming, and striped locking ([a620c28](https://github.com/id-data-foundry/data-foundry/commit/a620c289131ac0a3d5f6a3f6d7fb702e46d3e294))
- Added AiLaneLimiterTest (4 unit tests) ([a620c28](https://github.com/id-data-foundry/data-foundry/commit/a620c289131ac0a3d5f6a3f6d7fb702e46d3e294))
- Added GenericApiServiceStripedLockTest (1 unit test) ([a620c28](https://github.com/id-data-foundry/data-foundry/commit/a620c289131ac0a3d5f6a3f6d7fb702e46d3e294))
- Add 'change' event listener to #file-input to submit the upload form ([fa5650c](https://github.com/id-data-foundry/data-foundry/commit/fa5650c7d1f7e10d2ae7e209412a55ad5fd79cfa))
- Add CodingAgentUtils.resolveLocalProxyBasePath() to resolve the base ([e60e078](https://github.com/id-data-foundry/data-foundry/commit/e60e07830f667cd4fe1bffabf3da75f9264ceca9))
- Add CodingAgentUtils.resolveLocalProxyUrl(config) to resolve host, port, ([e60e078](https://github.com/id-data-foundry/data-foundry/commit/e60e07830f667cd4fe1bffabf3da75f9264ceca9))
- Add optional ConfigurationUtils.DF_CODINGAGENT_LOCAL_PROXY_URL config override. ([e60e078](https://github.com/id-data-foundry/data-foundry/commit/e60e07830f667cd4fe1bffabf3da75f9264ceca9))
- Add unit tests in CodingAgentUtilsTest covering reverse route basePath ([e60e078](https://github.com/id-data-foundry/data-foundry/commit/e60e07830f667cd4fe1bffabf3da75f9264ceca9))

### Changed
- Expand CompleteDSControllerPublicDownloadTest to test downloadFilePublic, downloadLatestFile, and downloadFile ([6f399b0](https://github.com/id-data-foundry/data-foundry/commit/6f399b022d98840fee27d7cc31213296bf9b93de))
- Verify cross-tenant isolation, unauthenticated redirects, and path traversal blocks across all actions ([6f399b0](https://github.com/id-data-foundry/data-foundry/commit/6f399b022d98840fee27d7cc31213296bf9b93de))
- Harden CompleteDS.getFileTemp to reject traversal characters (.., /, \) before sanitization ([6f399b0](https://github.com/id-data-foundry/data-foundry/commit/6f399b022d98840fee27d7cc31213296bf9b93de))
- Verify storage-level canonical containment for getFile and getFileTemp ([6f399b0](https://github.com/id-data-foundry/data-foundry/commit/6f399b022d98840fee27d7cc31213296bf9b93de))
- Enforce canonical folder containment and remove double URL decoding in CompleteDS.getFile ([9ff735b](https://github.com/id-data-foundry/data-foundry/commit/9ff735bcafc0ce1bac25712f4472781952d3f11e))
- Validate filenames and reject traversal sequences in CompleteDSController.downloadFilePublic ([9ff735b](https://github.com/id-data-foundry/data-foundry/commit/9ff735bcafc0ce1bac25712f4472781952d3f11e))
- Enforce active project checks and timing-safe public token validation ([9ff735b](https://github.com/id-data-foundry/data-foundry/commit/9ff735bcafc0ce1bac25712f4472781952d3f11e))
- Return explicit 404 on missing/invalid files instead of redirecting to internal views ([9ff735b](https://github.com/id-data-foundry/data-foundry/commit/9ff735bcafc0ce1bac25712f4472781952d3f11e))
- Direct SSE Streaming: Eliminate the ActorRef(16, dropTail) buffer and ([a620c28](https://github.com/id-data-foundry/data-foundry/commit/a620c289131ac0a3d5f6a3f6d7fb702e46d3e294))
- Per-Category Concurrency Lanes: Introduce AiLane and AiLaneLimiter to ([a620c28](https://github.com/id-data-foundry/data-foundry/commit/a620c289131ac0a3d5f6a3f6d7fb702e46d3e294))
- Eliminate Thread Parking: Convert UnmanagedAIApiController endpoints to ([a620c28](https://github.com/id-data-foundry/data-foundry/commit/a620c289131ac0a3d5f6a3f6d7fb702e46d3e294))
- Striped Credit Locks: Replace synchronized(datastore) in GenericApiService ([a620c28](https://github.com/id-data-foundry/data-foundry/commit/a620c289131ac0a3d5f6a3f6d7fb702e46d3e294))
- Honest HTTP Status Codes: Map error states accurately to 401 Unauthorized, ([a620c28](https://github.com/id-data-foundry/data-foundry/commit/a620c289131ac0a3d5f6a3f6d7fb702e46d3e294))
- Async DB Logging & Observability: Offload usage logging to a dedicated daemon ([a620c28](https://github.com/id-data-foundry/data-foundry/commit/a620c289131ac0a3d5f6a3f6d7fb702e46d3e294))
- Cleanup & Dead Code: Delete RemoteRequestsExecutionService and ([a620c28](https://github.com/id-data-foundry/data-foundry/commit/a620c289131ac0a3d5f6a3f6d7fb702e46d3e294))
- Verified all 11 tests pass under services.api.ai.* ([a620c28](https://github.com/id-data-foundry/data-foundry/commit/a620c289131ac0a3d5f6a3f6d7fb702e46d3e294))
- Reverted production yaml (#8) ([943909e](https://github.com/id-data-foundry/data-foundry/commit/943909ec61ac6fd0f862d809fdb749ae5735ae94))
- Dockerfile updates (#6) ([d2522cf](https://github.com/id-data-foundry/data-foundry/commit/d2522cfeb4728718f1ae7fe14d8653c20c95e9b7))
- Updated dockerfile ([d2522cf](https://github.com/id-data-foundry/data-foundry/commit/d2522cfeb4728718f1ae7fe14d8653c20c95e9b7))
- Made dockerfile platform agnostic ([d2522cf](https://github.com/id-data-foundry/data-foundry/commit/d2522cfeb4728718f1ae7fe14d8653c20c95e9b7))
- Updated documentation with build commands ([d2522cf](https://github.com/id-data-foundry/data-foundry/commit/d2522cfeb4728718f1ae7fe14d8653c20c95e9b7))
- Changed runner to support multi-arch ([d2522cf](https://github.com/id-data-foundry/data-foundry/commit/d2522cfeb4728718f1ae7fe14d8653c20c95e9b7))
- Update from as casing ([d2522cf](https://github.com/id-data-foundry/data-foundry/commit/d2522cfeb4728718f1ae7fe14d8653c20c95e9b7))
- Reset the upload form on 'htmx:afterRequest' to allow re-uploading ([fa5650c](https://github.com/id-data-foundry/data-foundry/commit/fa5650c7d1f7e10d2ae7e209412a55ad5fd79cfa))
- Delegate clicks on .file-uploader to open the file dialog when ([fa5650c](https://github.com/id-data-foundry/data-foundry/commit/fa5650c7d1f7e10d2ae7e209412a55ad5fd79cfa))
- Consolidate file type filtering logic into filterValidFiles() for ([fa5650c](https://github.com/id-data-foundry/data-foundry/commit/fa5650c7d1f7e10d2ae7e209412a55ad5fd79cfa))
- Decouple CodingAgentController.getDatasetFlow from incoming browser ([e60e078](https://github.com/id-data-foundry/data-foundry/commit/e60e07830f667cd4fe1bffabf3da75f9264ceca9))

### Fixed
- Fixed coding agent view chat window sizing ([3b94b2b](https://github.com/id-data-foundry/data-foundry/commit/3b94b2b9da8eddb38367b9ebf90f33caae09ebef))
- Fixed issue with jekyll versioning ([d2522cf](https://github.com/id-data-foundry/data-foundry/commit/d2522cfeb4728718f1ae7fe14d8653c20c95e9b7))
- Trigger upload on file dialog selection and reset form ([fa5650c](https://github.com/id-data-foundry/data-foundry/commit/fa5650c7d1f7e10d2ae7e209412a55ad5fd79cfa))
- Resolve internal AI proxy via df.base_url and Play routes ([e60e078](https://github.com/id-data-foundry/data-foundry/commit/e60e07830f667cd4fe1bffabf3da75f9264ceca9))

## [v0.9.30] - 2026-09-07
### Security
- Add SecurityModule and mock SSO configuration to test/resources/tests.conf ([bda37ae](https://github.com/id-data-foundry/data-foundry/commit/bda37ae9377cb9a3edb630f4c200d9f41c2c6118))

### Added
- Add Pushover channel with hostname identification ([3cd9b8a](https://github.com/id-data-foundry/data-foundry/commit/3cd9b8abcaa99c9d4c9d3a138db52175c229f3e9))
- Add configuration keys, defaults, and validation checks in ConfigurationUtils and Configurator ([3cd9b8a](https://github.com/id-data-foundry/data-foundry/commit/3cd9b8abcaa99c9d4c9d3a138db52175c229f3e9))
- Add unit tests verifying configuration, default URL, priority mapping, hostname formatting, and manager registration ([3cd9b8a](https://github.com/id-data-foundry/data-foundry/commit/3cd9b8abcaa99c9d4c9d3a138db52175c229f3e9))
- Add HIGH severity constant to NotificationLevel enum. ([96bf844](https://github.com/id-data-foundry/data-foundry/commit/96bf84481a0ccb1046ab3d9743511a9c2af6ce02))
- Add unit tests in UnmanagedAIApiServiceOfflineAlertTest covering empty string, empty data array, and error response scenarios. ([96bf844](https://github.com/id-data-foundry/data-foundry/commit/96bf84481a0ccb1046ab3d9743511a9c2af6ce02))
- Add ProjectsControllerExtendTest to verify that extending project duration ([bda37ae](https://github.com/id-data-foundry/data-foundry/commit/bda37ae9377cb9a3edb630f4c200d9f41c2c6118))
- Add UnmanagedAIApiServiceOfflineAlertTest for AI health check state machine and alerting thresholds ([b2cb7ed](https://github.com/id-data-foundry/data-foundry/commit/b2cb7ed4f765d86407377fb2a131b41ee1d40e0a))
- Add NotificationManagerTest covering Slack/Ntfy notification channels, static facade, and in-memory history buffer ([b2cb7ed](https://github.com/id-data-foundry/data-foundry/commit/b2cb7ed4f765d86407377fb2a131b41ee1d40e0a))
- Implement multi-channel notification infrastructure and AI offline alerting ([3c8c72f](https://github.com/id-data-foundry/data-foundry/commit/3c8c72f4fce600b445fe1eea8c66eddda72e2cf3))
- Add versatile notification framework under HOCON key `df.notifications`: ([3c8c72f](https://github.com/id-data-foundry/data-foundry/commit/3c8c72f4fce600b445fe1eea8c66eddda72e2cf3))
- Add Ntfy channel with customizable server, topic, auth token, and priority mapping (mapping TRACE/DEBUG to min priority 1). ([3c8c72f](https://github.com/id-data-foundry/data-foundry/commit/3c8c72f4fce600b445fe1eea8c66eddda72e2cf3))
- Add AI service health monitoring in UnmanagedAIApiService: ([3c8c72f](https://github.com/id-data-foundry/data-foundry/commit/3c8c72f4fce600b445fe1eea8c66eddda72e2cf3))
- Add system notification settings display for Slack, Ntfy, and AI offline alerts. ([3c8c72f](https://github.com/id-data-foundry/data-foundry/commit/3c8c72f4fce600b445fe1eea8c66eddda72e2cf3))
- Add live table rendering the last 50 notifications with severity badges and delivery status. ([3c8c72f](https://github.com/id-data-foundry/data-foundry/commit/3c8c72f4fce600b445fe1eea8c66eddda72e2cf3))
- Add Person.getOwnAndCollabProjectsCount() and use in users.scala.html ([067028e](https://github.com/id-data-foundry/data-foundry/commit/067028e7783b4585027eee9556c3b3ca4452aff3))
- Add countSubscriptions() and hasSubscription() to OOCSIService ([87c0a3c](https://github.com/id-data-foundry/data-foundry/commit/87c0a3c9ed6155774b8a2ad2963609c8c6a1ab5e))

### Changed
- Implement PushoverNotificationChannel sending JSON payloads to Pushover REST API ([3cd9b8a](https://github.com/id-data-foundry/data-foundry/commit/3cd9b8abcaa99c9d4c9d3a138db52175c229f3e9))
- Include server hostname in notification title for origin identification ([3cd9b8a](https://github.com/id-data-foundry/data-foundry/commit/3cd9b8abcaa99c9d4c9d3a138db52175c229f3e9))
- Map NotificationLevel to Pushover priority levels (CRITICAL/ERROR -> 1, WARNING -> 0, INFO -> default, TRACE/DEBUG -> -1) ([3cd9b8a](https://github.com/id-data-foundry/data-foundry/commit/3cd9b8abcaa99c9d4c9d3a138db52175c229f3e9))
- Display Pushover settings in the admin configuration template ([3cd9b8a](https://github.com/id-data-foundry/data-foundry/commit/3cd9b8abcaa99c9d4c9d3a138db52175c229f3e9))
- Register Pushover channel in NotificationManager alongside Slack and Ntfy ([3cd9b8a](https://github.com/id-data-foundry/data-foundry/commit/3cd9b8abcaa99c9d4c9d3a138db52175c229f3e9))
- Validate `/models` response in UnmanagedAIApiService.refresh(), treating empty responses, ([96bf844](https://github.com/id-data-foundry/data-foundry/commit/96bf84481a0ccb1046ab3d9743511a9c2af6ce02))
- Prevent false ONLINE recovery transitions and alerts when receiving empty model responses. ([96bf844](https://github.com/id-data-foundry/data-foundry/commit/96bf84481a0ccb1046ab3d9743511a9c2af6ce02))
- Update LocalModelMetadata.updateModels() to return boolean status indicating if models were loaded. ([96bf844](https://github.com/id-data-foundry/data-foundry/commit/96bf84481a0ccb1046ab3d9743511a9c2af6ce02))
- Modernize priority mapping switch in NtfyNotificationChannel to arrow syntax with unqualified ([96bf844](https://github.com/id-data-foundry/data-foundry/commit/96bf84481a0ccb1046ab3d9743511a9c2af6ce02))
- Send application start and shutdown notifications with NotificationLevel.HIGH in both dev and prod in RefreshTask. ([96bf844](https://github.com/id-data-foundry/data-foundry/commit/96bf84481a0ccb1046ab3d9743511a9c2af6ce02))
- Inject DatasetUpdateQueue into ProjectsController and enqueue each dataset ([bda37ae](https://github.com/id-data-foundry/data-foundry/commit/bda37ae9377cb9a3edb630f4c200d9f41c2c6118))
- Inject DatasetUpdateQueue into DatasetApiController and enqueue datasets ([bda37ae](https://github.com/id-data-foundry/data-foundry/commit/bda37ae9377cb9a3edb630f4c200d9f41c2c6118))
- Update DatasetApiControllerTest to verify that add, edit, and delete API ([bda37ae](https://github.com/id-data-foundry/data-foundry/commit/bda37ae9377cb9a3edb630f4c200d9f41c2c6118))
- Consolidate unit and service tests into DataFoundry/test ([b2cb7ed](https://github.com/id-data-foundry/data-foundry/commit/b2cb7ed4f765d86407377fb2a131b41ee1d40e0a))
- Update CacheTest with required mock API secret keys and base URL ([b2cb7ed](https://github.com/id-data-foundry/data-foundry/commit/b2cb7ed4f765d86407377fb2a131b41ee1d40e0a))
- Make LocalModelMetadataTest working-directory resilient for JSON test fixture resolution ([b2cb7ed](https://github.com/id-data-foundry/data-foundry/commit/b2cb7ed4f765d86407377fb2a131b41ee1d40e0a))
- Update tests.conf with play.ebean.generateEvolutionsScripts = false, common.Routes routing, and mock keys ([b2cb7ed](https://github.com/id-data-foundry/data-foundry/commit/b2cb7ed4f765d86407377fb2a131b41ee1d40e0a))
- Support multi-channel dispatch with Slack and Ntfy.io channels. ([3c8c72f](https://github.com/id-data-foundry/data-foundry/commit/3c8c72f4fce600b445fe1eea8c66eddda72e2cf3))
- Support optional Slack URL with automatic fallback to base webhook URL if key is supplied, plus legacy fallback to `df.vendor.slack.channel`. ([3c8c72f](https://github.com/id-data-foundry/data-foundry/commit/3c8c72f4fce600b445fe1eea8c66eddda72e2cf3))
- Implement 50-item in-memory ring buffer for recent notifications without requiring persistence. ([3c8c72f](https://github.com/id-data-foundry/data-foundry/commit/3c8c72f4fce600b445fe1eea8c66eddda72e2cf3))
- Alert with CRITICAL severity when the AI backend fails 2 consecutive health checks. ([3c8c72f](https://github.com/id-data-foundry/data-foundry/commit/3c8c72f4fce600b445fe1eea8c66eddda72e2cf3))
- Prevent alert storms by withholding duplicate alerts while offline. ([3c8c72f](https://github.com/id-data-foundry/data-foundry/commit/3c8c72f4fce600b445fe1eea8c66eddda72e2cf3))
- Alert with INFO severity upon recovery. ([3c8c72f](https://github.com/id-data-foundry/data-foundry/commit/3c8c72f4fce600b445fe1eea8c66eddda72e2cf3))
- Modernize dependency injection across controllers and backend services: ([3c8c72f](https://github.com/id-data-foundry/data-foundry/commit/3c8c72f4fce600b445fe1eea8c66eddda72e2cf3))
- Introduce SystemNotificationService with Guice singleton NotificationManager. ([3c8c72f](https://github.com/id-data-foundry/data-foundry/commit/3c8c72f4fce600b445fe1eea8c66eddda72e2cf3))
- Inject SystemNotificationService into AbstractAsyncController for direct inheritance by derived controllers. ([3c8c72f](https://github.com/id-data-foundry/data-foundry/commit/3c8c72f4fce600b445fe1eea8c66eddda72e2cf3))
- Inject SystemNotificationService into ParticipationController and backend services (RefreshTask, NotificationService, FitBitService, GoogleFitService, OOCSIService, ([3c8c72f](https://github.com/id-data-foundry/data-foundry/commit/3c8c72f4fce600b445fe1eea8c66eddda72e2cf3))
- Map routine caught exceptions to TRACE severity so they don't trigger on-call alarms. ([3c8c72f](https://github.com/id-data-foundry/data-foundry/commit/3c8c72f4fce600b445fe1eea8c66eddda72e2cf3))
- Reserve static Notifications.call facade strictly for reflectively instantiated Ebean database entities in models/. ([3c8c72f](https://github.com/id-data-foundry/data-foundry/commit/3c8c72f4fce600b445fe1eea8c66eddda72e2cf3))
- Deprecate services.slack.Slack in favor of Notifications.send. ([3c8c72f](https://github.com/id-data-foundry/data-foundry/commit/3c8c72f4fce600b445fe1eea8c66eddda72e2cf3))
- Update Admin UI (views.admin.configuration): ([3c8c72f](https://github.com/id-data-foundry/data-foundry/commit/3c8c72f4fce600b445fe1eea8c66eddda72e2cf3))
- Update DatasetsController.hasOOCSIChannel to validate channels against in-memory active subscriptions ([87c0a3c](https://github.com/id-data-foundry/data-foundry/commit/87c0a3c9ed6155774b8a2ad2963609c8c6a1ab5e))
- Restore hasOOCSIChannel route in common.routes ([87c0a3c](https://github.com/id-data-foundry/data-foundry/commit/87c0a3c9ed6155774b8a2ad2963609c8c6a1ab5e))
- Eliminates full dataset table scans on every change of the OOCSI output channel input ([87c0a3c](https://github.com/id-data-foundry/data-foundry/commit/87c0a3c9ed6155774b8a2ad2963609c8c6a1ab5e))
- Filter public, non-archived, non-frozen projects at the database layer during search re-indexing in SearchService ([5c30c27](https://github.com/id-data-foundry/data-foundry/commit/5c30c278ddc35187c3e8d96aeddfedf1628f8dea))
- Exclude frozen projects directly via query in DatabaseBackupService ([5c30c27](https://github.com/id-data-foundry/data-foundry/commit/5c30c278ddc35187c3e8d96aeddfedf1628f8dea))
- Eliminates wasteful full-table fetches and redundant in-memory filtering ([5c30c27](https://github.com/id-data-foundry/data-foundry/commit/5c30c278ddc35187c3e8d96aeddfedf1628f8dea))
- Optimize DBController index and generateData guards to use Project.find.query().exists() ([2d8c818](https://github.com/id-data-foundry/data-foundry/commit/2d8c8187884de11ace4e8310c2d010c049feceb6))
- Optimize HomeController dev initialization check to use !Project.find.query().exists() ([2d8c818](https://github.com/id-data-foundry/data-foundry/commit/2d8c8187884de11ace4e8310c2d010c049feceb6))
- Prevents full table scans and redundant JVM entity hydration ([2d8c818](https://github.com/id-data-foundry/data-foundry/commit/2d8c8187884de11ace4e8310c2d010c049feceb6))

### Performance
- Remove redundant refresh() calls from Project and Person ([067028e](https://github.com/id-data-foundry/data-foundry/commit/067028e7783b4585027eee9556c3b3ca4452aff3))
- Replace Dataset.find.all() with in-memory OOCSIService subscription check ([87c0a3c](https://github.com/id-data-foundry/data-foundry/commit/87c0a3c9ed6155774b8a2ad2963609c8c6a1ab5e))
- Push project filters to SQL query in SearchService and DatabaseBackupService ([5c30c27](https://github.com/id-data-foundry/data-foundry/commit/5c30c278ddc35187c3e8d96aeddfedf1628f8dea))
- Replace wasteful Project.find.all() checks with query().exists() ([2d8c818](https://github.com/id-data-foundry/data-foundry/commit/2d8c8187884de11ace4e8310c2d010c049feceb6))

### Removed
- Remove refresh() calls from Project permission and membership methods ([067028e](https://github.com/id-data-foundry/data-foundry/commit/067028e7783b4585027eee9556c3b3ca4452aff3))
- Remove c.getProject().refresh() from Person.getOwnAndCollabProjects() ([067028e](https://github.com/id-data-foundry/data-foundry/commit/067028e7783b4585027eee9556c3b3ca4452aff3))

### Fixed
- Trigger AI offline alerts on empty/error responses and refine channel handling ([96bf844](https://github.com/id-data-foundry/data-foundry/commit/96bf84481a0ccb1046ab3d9743511a9c2af6ce02))
- Enqueue datasets to DatasetUpdateQueue on project extend and API CRUD ([bda37ae](https://github.com/id-data-foundry/data-foundry/commit/bda37ae9377cb9a3edb630f4c200d9f41c2c6118))
- Fixed ws connections for production deployments behind a WAF ([b8cc588](https://github.com/id-data-foundry/data-foundry/commit/b8cc588234b82637d5b212f580a3fb0cddf1614c))

## [v0.9.29] - 2026-08-28
### Added
- Report complete datasets as existing and include dataset API routes in project metadata ([8a2cf11](https://github.com/id-data-foundry/data-foundry/commit/8a2cf11b1930fa6ddb6b6e6b0125443fa83dd5a4))
- Add unit tests in CodingAgentUtilsTest for dataset type mapping and route generation ([8a2cf11](https://github.com/id-data-foundry/data-foundry/commit/8a2cf11b1930fa6ddb6b6e6b0125443fa83dd5a4))
- Add DatasetLengthConstraintTest verifying oversized string inputs for datasets ([798dc45](https://github.com/id-data-foundry/data-foundry/commit/798dc45a23191bb17acf871c8926f10cc4b54feb))
- Add unit tests for DatasetUpdateQueue ([2b3b655](https://github.com/id-data-foundry/data-foundry/commit/2b3b65512f8609f679f511cad8abf6d40941539c))

### Changed
- Name COMPLETE datasets as EXISTING in get_project_metadata to match UI naming conventions ([8a2cf11](https://github.com/id-data-foundry/data-foundry/commit/8a2cf11b1930fa6ddb6b6e6b0125443fa83dd5a4))
- Attach apiRoutes map with standard HTTP methods and URL paths to each dataset in metadata ([8a2cf11](https://github.com/id-data-foundry/data-foundry/commit/8a2cf11b1930fa6ddb6b6e6b0125443fa83dd5a4))
- Update coding agent knowledge documentation to reflect apiRoutes and dataset types ([8a2cf11](https://github.com/id-data-foundry/data-foundry/commit/8a2cf11b1930fa6ddb6b6e6b0125443fa83dd5a4))
- Align LinkedDS subclasses with DB schema column length constraints ([798dc45](https://github.com/id-data-foundry/data-foundry/commit/798dc45a23191bb17acf871c8926f10cc4b54feb))
- Use `nss(value, maxLength)` across LinkedDS subclasses to prevent SQL
      exceptions when field values exceed table column lengths: ([798dc45](https://github.com/id-data-foundry/data-foundry/commit/798dc45a23191bb17acf871c8926f10cc4b54feb))
- EntityDS: enforce limits on resource_id (63), token (63), and pp1..3 (255) ([798dc45](https://github.com/id-data-foundry/data-foundry/commit/798dc45a23191bb17acf871c8926f10cc4b54feb))
- CompleteDS: enforce limit on description (255) in updateRecord ([798dc45](https://github.com/id-data-foundry/data-foundry/commit/798dc45a23191bb17acf871c8926f10cc4b54feb))
- ExpSamplingDS: enforce limits on pp1..3 (255) and data (1024) in importFileContents ([798dc45](https://github.com/id-data-foundry/data-foundry/commit/798dc45a23191bb17acf871c8926f10cc4b54feb))
- FitbitDS: enforce limits on user_id (50), pp1..3 (255), sleep (10), and activity (16383) ([798dc45](https://github.com/id-data-foundry/data-foundry/commit/798dc45a23191bb17acf871c8926f10cc4b54feb))
- GoogleFitDS: enforce limits on user_id (50) and pp1..3 (255) ([798dc45](https://github.com/id-data-foundry/data-foundry/commit/798dc45a23191bb17acf871c8926f10cc4b54feb))
- Move OOCSI notification in CompleteDS.addRecord outside the error catch block ([798dc45](https://github.com/id-data-foundry/data-foundry/commit/798dc45a23191bb17acf871c8926f10cc4b54feb))
- Code clean-up coding agent ([833aa6d](https://github.com/id-data-foundry/data-foundry/commit/833aa6db383364945abed0a3b99affc5661ce7ea))
- Replace full database scans in OOCSIService and JSExecutorService with an in-memory change queue (DatasetUpdateQueue) ([2b3b655](https://github.com/id-data-foundry/data-foundry/commit/2b3b65512f8609f679f511cad8abf6d40941539c))
- Inject DatasetUpdateQueue into relevant controllers (ActorController, ScriptApiController, DatasetsController, TimeseriesDSController, EntityDSController, ProjectLifecycleService) to ([2b3b655](https://github.com/id-data-foundry/data-foundry/commit/2b3b65512f8609f679f511cad8abf6d40941539c))
- Handle cron triggers in JSExecutorService directly from in-memory schedule without database queries ([2b3b655](https://github.com/id-data-foundry/data-foundry/commit/2b3b65512f8609f679f511cad8abf6d40941539c))
- Eliminate all database queries during idle 10-second refresh intervals ([2b3b655](https://github.com/id-data-foundry/data-foundry/commit/2b3b65512f8609f679f511cad8abf6d40941539c))

### Performance
- Optimize refresh task with in-memory dataset change queue ([2b3b655](https://github.com/id-data-foundry/data-foundry/commit/2b3b65512f8609f679f511cad8abf6d40941539c))

### Fixed
- Fix parameter bug in EntityDS.updateItem where pp2 was duplicated instead of pp3 ([798dc45](https://github.com/id-data-foundry/data-foundry/commit/798dc45a23191bb17acf871c8926f10cc4b54feb))

## [v0.9.28] - 2026-08-28
### Security
- Sanitize path variants and strip thinking tags across: ([6c12a6f](https://github.com/id-data-foundry/data-foundry/commit/6c12a6f48e3ecf774895fe9798791ce7c084059c))

### Added
- Allow independent model configuration for coding subagent ([833b0e4](https://github.com/id-data-foundry/data-foundry/commit/833b0e41e1d55639412fe8e82af31488adaef405))
- Add `DF_AI_MODEL_CODING_SUBAGENT` (`df.processing.ai.models.coding_subagent`) in `ConfigurationUtils` ([833b0e4](https://github.com/id-data-foundry/data-foundry/commit/833b0e41e1d55639412fe8e82af31488adaef405))
- Support team chat and @boter summoning ([081215b](https://github.com/id-data-foundry/data-foundry/commit/081215b59f51af740dc7f76da10d8535ad6e8956))
- Add configurable bot and sub-agent identities in CodingAgentUtils: ([081215b](https://github.com/id-data-foundry/data-foundry/commit/081215b59f51af740dc7f76da10d8535ad6e8956))
- Add comprehensive unit tests in CodingAgentUtilsTest ([081215b](https://github.com/id-data-foundry/data-foundry/commit/081215b59f51af740dc7f76da10d8535ad6e8956))
- Sanitize dataset and project root filesystem paths in agent output ([6c12a6f](https://github.com/id-data-foundry/data-foundry/commit/6c12a6f48e3ecf774895fe9798791ce7c084059c))
- Add CodingAgentUtils in utils.tools with filterDatasetPath and cleanThinkingTags helpers ([6c12a6f](https://github.com/id-data-foundry/data-foundry/commit/6c12a6f48e3ecf774895fe9798791ce7c084059c))
- Add path safety directives to main agent and sub-agent system prompt templates ([6c12a6f](https://github.com/id-data-foundry/data-foundry/commit/6c12a6f48e3ecf774895fe9798791ce7c084059c))
- Add comprehensive JUnit tests in CodingAgentUtilsTest covering dataset paths, workspace subfolders, project root paths, and edge cases ([6c12a6f](https://github.com/id-data-foundry/data-foundry/commit/6c12a6f48e3ecf774895fe9798791ce7c084059c))
- Add `CacheTest` unit test suite verifying bean cache hits, natural key ([6cfae42](https://github.com/id-data-foundry/data-foundry/commit/6cfae42194535c2eeccb60d652987a4f19482664))
- Add a sub-agent implementation to offload model calls ([24b160d](https://github.com/id-data-foundry/data-foundry/commit/24b160d8897e7774f05f98c0dcc9c129adfe8ea5))
- Refactor to multi-agent architecture for planning and execution ([8eead8c](https://github.com/id-data-foundry/data-foundry/commit/8eead8c09b6e1fcee09be7974ad079cb4ac99ddc))
- Add friendly notifications during sub-agent task execution while maintaining live WebSocket file-sync broadcasts. ([8eead8c](https://github.com/id-data-foundry/data-foundry/commit/8eead8c09b6e1fcee09be7974ad079cb4ac99ddc))
- Log user email on AI proxy requests via X-API-User header ([2ca79be](https://github.com/id-data-foundry/data-foundry/commit/2ca79be8a989647188048b3b46126571df1f449a))
- Added logging of AI requests across the entire platform
added visualization in admin area for AI requests
added session_export visualization for coding agent view
improved system prompt ([39ff14d](https://github.com/id-data-foundry/data-foundry/commit/39ff14dfc26c4906a2e34c0e1e583a52545e58dc))

### Changed
- Aligned styling and wording of coding agent index ([43fbbbd](https://github.com/id-data-foundry/data-foundry/commit/43fbbbd8e9f3610cea6f81af3f4c931767632a4f))
- Updated system prompts ([ada0656](https://github.com/id-data-foundry/data-foundry/commit/ada06567800ddbd3a81b00286cac27c04df19610))
- Independently resolve and instantiate dedicated `OpenAIChatModel` instances for the Main Agent and Coding Sub-Agent in `CodingAgentController` ([833b0e4](https://github.com/id-data-foundry/data-foundry/commit/833b0e41e1d55639412fe8e82af31488adaef405))
- Fallback subagent model to `df.processing.ai.models.coding` -> `df.processing.ai.models.default` -> `qwen/qwen3.6-27b` ([833b0e4](https://github.com/id-data-foundry/data-foundry/commit/833b0e41e1d55639412fe8e82af31488adaef405))
- Main Agent: 'boter' (displayed as '@boter 🧈') summoned with '@boter' ([081215b](https://github.com/id-data-foundry/data-foundry/commit/081215b59f51af740dc7f76da10d8535ad6e8956))
- Sub-Agent: 'comrade roomboter' with configurable start and completion notifications ([081215b](https://github.com/id-data-foundry/data-foundry/commit/081215b59f51af740dc7f76da10d8535ad6e8956))
- Enable team members to chat without triggering the AI agent on every message ([081215b](https://github.com/id-data-foundry/data-foundry/commit/081215b59f51af740dc7f76da10d8535ad6e8956))
- Append all team messages to persistent session history and the agent's contextual memory (AgentState) ([081215b](https://github.com/id-data-foundry/data-foundry/commit/081215b59f51af740dc7f76da10d8535ad6e8956))
- Update frontend UI placeholder, typing indicator, and message headers for '@boter 🧈' ([081215b](https://github.com/id-data-foundry/data-foundry/commit/081215b59f51af740dc7f76da10d8535ad6e8956))
- Return HTTP 404 (notFound) in CompleteDSController.downloadFile when a file is missing instead of redirecting to the dataset HTML view ([7028eb8](https://github.com/id-data-foundry/data-foundry/commit/7028eb8fdd3f7918f42f7344ebbd4ef505cb658d))
- Guard editor fetch in view.scala.html against non-positive file IDs and redirect responses ([7028eb8](https://github.com/id-data-foundry/data-foundry/commit/7028eb8fdd3f7918f42f7344ebbd4ef505cb658d))
- Replace all hardcoded path strings in view.scala.html with Play reverse routes for file downloads, session history, file list polling, option values, and WebSocket connection ([7028eb8](https://github.com/id-data-foundry/data-foundry/commit/7028eb8fdd3f7918f42f7344ebbd4ef505cb658d))
- Show clean placeholder in editor when opening a dataset with no files ([7028eb8](https://github.com/id-data-foundry/data-foundry/commit/7028eb8fdd3f7918f42f7344ebbd4ef505cb658d))
- Auto-select first editable file on the server in CodingAgentController.view when files exist ([7028eb8](https://github.com/id-data-foundry/data-foundry/commit/7028eb8fdd3f7918f42f7344ebbd4ef505cb658d))
- Harden ReadOnlyFileTool and FileMutationTool against path traversal (..) and enforce canonical containment within the dataset directory ([7028eb8](https://github.com/id-data-foundry/data-foundry/commit/7028eb8fdd3f7918f42f7344ebbd4ef505cb658d))
- Filter dataset directories, .agentscope workspace folders, and project/application root paths (new File(""), user.dir) to generic "PATH" ([6c12a6f](https://github.com/id-data-foundry/data-foundry/commit/6c12a6f48e3ecf774895fe9798791ce7c084059c))
- Main agent WebSocket stream and persistent session history ([6c12a6f](https://github.com/id-data-foundry/data-foundry/commit/6c12a6f48e3ecf774895fe9798791ce7c084059c))
- Sub-agent delegation tool (execute_coding_task) return values and error messages ([6c12a6f](https://github.com/id-data-foundry/data-foundry/commit/6c12a6f48e3ecf774895fe9798791ce7c084059c))
- History bootstrap fallback from AgentState ([6c12a6f](https://github.com/id-data-foundry/data-foundry/commit/6c12a6f48e3ecf774895fe9798791ce7c084059c))
- Enable bean caching and natural key indexing on `Person` (`email`), ([6cfae42](https://github.com/id-data-foundry/data-foundry/commit/6cfae42194535c2eeccb60d652987a4f19482664))
- Enable L2 caching on study resource entities (`Device`, `Wearable`, ([6cfae42](https://github.com/id-data-foundry/data-foundry/commit/6cfae42194535c2eeccb60d652987a4f19482664))
- Keep append-only/high-write entities (`LabNotesEntry`, `Analytics`) ([6cfae42](https://github.com/id-data-foundry/data-foundry/commit/6cfae42194535c2eeccb60d652987a4f19482664))
- Replace flawed regex `(?s)(<think>)?.*?</think>` which accidentally stripped text prior to tags ([c2579fa](https://github.com/id-data-foundry/data-foundry/commit/c2579fa8f67e911606631a0b23ea7171a48b138c))
- Support complete `<think>...</think>` blocks, unclosed `<think>` tags, and start-of-string orphaned `</think>` tags ([c2579fa](https://github.com/id-data-foundry/data-foundry/commit/c2579fa8f67e911606631a0b23ea7171a48b138c))
- Separate agent responsibilities into Main Chat Agent (Planner) and Coding Sub-Agent (Developer). ([8eead8c](https://github.com/id-data-foundry/data-foundry/commit/8eead8c09b6e1fcee09be7974ad079cb4ac99ddc))
- Grant Main Agent read-only tools (read_file, list_files, get_project_metadata) and delegation tool (execute_coding_task) for conversational planning. ([8eead8c](https://github.com/id-data-foundry/data-foundry/commit/8eead8c09b6e1fcee09be7974ad079cb4ac99ddc))
- Restrict file mutation tools (write_file, edit_file) exclusively to the Coding Sub-Agent. ([8eead8c](https://github.com/id-data-foundry/data-foundry/commit/8eead8c09b6e1fcee09be7974ad079cb4ac99ddc))
- Update system prompt templates for conversational planning and precise sub-agent code execution. ([8eead8c](https://github.com/id-data-foundry/data-foundry/commit/8eead8c09b6e1fcee09be7974ad079cb4ac99ddc))
- Extended max_tokens for local AI examples to ensure that we receive output for thinking models
extended max_token defaults in the local-ai library ([809d12e](https://github.com/id-data-foundry/data-foundry/commit/809d12e2cb9f92e537753b68e05a7d6b0ba3af1e))

### Performance
- Enable Ebean Level 2 (L2) caching with natural keys ([6cfae42](https://github.com/id-data-foundry/data-foundry/commit/6cfae42194535c2eeccb60d652987a4f19482664))

### Fixed
- Fixed reference bug in local-ai library ([2d313e8](https://github.com/id-data-foundry/data-foundry/commit/2d313e80b786a899c6786a21a9ca73348942c2f4))
- Resolve empty dataset editor display, use reverse routes, and harden file access ([7028eb8](https://github.com/id-data-foundry/data-foundry/commit/7028eb8fdd3f7918f42f7344ebbd4ef505cb658d))
- Make thinking tag cleaning robust against edge cases ([c2579fa](https://github.com/id-data-foundry/data-foundry/commit/c2579fa8f67e911606631a0b23ea7171a48b138c))

## [v0.9.27] - 2026-08-28
### Added
- Added file chooser reload when coding agent creates new files ([408f90f](https://github.com/id-data-foundry/data-foundry/commit/408f90f0ce586d49b8b41be5b6a6900edb8de7a1))
- Added database compaction tool for admin ([7ba105b](https://github.com/id-data-foundry/data-foundry/commit/7ba105bc2ecc71dc54e203049bdc7440512bb928))
- Added correct syntax highlighting for .md files to standard code editor
disabled load app button for non-HTML files in coding agent ([d33670c](https://github.com/id-data-foundry/data-foundry/commit/d33670caf635e67a9a009e4e57259627ab437815))

### Changed
- AI models are now configurable from the application.conf by purpose, e.g., default, coding, text, translate ([c927a02](https://github.com/id-data-foundry/data-foundry/commit/c927a02042d6105f309dabcc3983896e92c9de48))
- Enabled AI thinking notifications (while running) to appear after reload of coding agent ([7a6a34d](https://github.com/id-data-foundry/data-foundry/commit/7a6a34d52ce548a9603d1d8f6f4dd867a7ec186f))
- Disabled cohere model due to processing problems ([e261f72](https://github.com/id-data-foundry/data-foundry/commit/e261f7281cf0aac731aeb09d58226f3584a81289))
- Coding agent editor now opens .gg files in Starboard ([ae1ff06](https://github.com/id-data-foundry/data-foundry/commit/ae1ff06a76d954808a648933e15a1ea12a16af37))
- Upgraded prism.js to 1.30.0
added markdown syntax highlighting ([1753d17](https://github.com/id-data-foundry/data-foundry/commit/1753d17c16679c4ba028ff929e4741f044d48d61))
- Improved wording and file handling in transcription processor
increased timeouts
added word document download
removed PDF from transcription
added .m4a upload format ([1aa9bf0](https://github.com/id-data-foundry/data-foundry/commit/1aa9bf06762760a46c6b0e04fb7dd6baa4ced937))
- Use TRUNCATE TABLE instead of DELETE FROM during dataset resets ([eedaf7c](https://github.com/id-data-foundry/data-foundry/commit/eedaf7c3f45e565a67090cfa9ba71ffa67b294cf))
- Replaced bulk DELETE FROM queries with TRUNCATE TABLE in LinkedDS, ExpSamplingDS, MediaDS, and MovementDS. ([eedaf7c](https://github.com/id-data-foundry/data-foundry/commit/eedaf7c3f45e565a67090cfa9ba71ffa67b294cf))
- Import QueueOfferResult and check the returned status of Pekko queue offer operations in TimeseriesDS.java and LinkedDS.java. ([e2c8287](https://github.com/id-data-foundry/data-foundry/commit/e2c828743b6cb8c531414be69ea85b621c57c1c2))
- Abort the database ResultSet iteration loop early if the offering result is not 'enqueued' (i.e., if the queue is closed or failed due to a
  cancelled client connection). ([e2c8287](https://github.com/id-data-foundry/data-foundry/commit/e2c828743b6cb8c531414be69ea85b621c57c1c2))
- This releases Ebean transactions, database connections, and cursors immediately instead of continuing to read the entire dataset, preventing MVCC
  history database bloating on client cancellation. ([e2c8287](https://github.com/id-data-foundry/data-foundry/commit/e2c828743b6cb8c531414be69ea85b621c57c1c2))

### Performance
- This optimizes performance, resets auto-increment primary key sequences, and immediately reclaims H2 storage pages without generating massive transaction log bloat. ([eedaf7c](https://github.com/id-data-foundry/data-foundry/commit/eedaf7c3f45e565a67090cfa9ba71ffa67b294cf))

### Removed
- Removed stats for landing page, added quick explanation and link to data-foundry.net ([c5bbe9b](https://github.com/id-data-foundry/data-foundry/commit/c5bbe9bc5516427c9933080b2c7f1ef912cf81b0))

### Fixed
- Fixed syntax highlighting for coding agent editor ([e414ff1](https://github.com/id-data-foundry/data-foundry/commit/e414ff1e62217e5198ad56ae8202bf813cce86f1))
- Fix database bloat by aborting export loops early on client disconnect ([e2c8287](https://github.com/id-data-foundry/data-foundry/commit/e2c828743b6cb8c531414be69ea85b621c57c1c2))

## [v0.9.26] - 2026-06-28
### Added
- Added workflow diagrams for remaining dataset type ([d8ba983](https://github.com/id-data-foundry/data-foundry/commit/d8ba983b77fa0f3070f2b70a039819b247d2abc3))
- Added /models endpoint JSON format to support OpenAI API and LiteLLM model routers, incl test ([6f00a4f](https://github.com/id-data-foundry/data-foundry/commit/6f00a4fb063b570271a7c1c6cee10b6263282b7f))
- Added file upload API to CompleteDS, including config tab, view examples and docs
added file upload to User JS API
fixed timestamp retrieval in CompleteDS
migrated Entity howto docs jQuery JS to vanilla
added IoT dataset howtos for bulk JSON upload ([4fdf418](https://github.com/id-data-foundry/data-foundry/commit/4fdf418186a097d8c19e15de0762f2c2a64e3138))
- Added bulk data upload option via JSON Arrays for IoT dataset
removed activity from URL in examples (still supported in code) ([64afbd1](https://github.com/id-data-foundry/data-foundry/commit/64afbd164d95fe53dcc9e48fa38c0d8979fb6595))
- Added agent context reloading in case of an updated AGENTS.md file in workspace ([c7b9251](https://github.com/id-data-foundry/data-foundry/commit/c7b925156247f0d1f5f2ac14382ba69080660aea))
- Added nginx configuration for connection upgrade (for websockets) ([c436b24](https://github.com/id-data-foundry/data-foundry/commit/c436b24c85c34d8bfa396d67637374c56e25e388))
- Added a first version of a coding agent ([57e49f5](https://github.com/id-data-foundry/data-foundry/commit/57e49f599f36ec6b1b3c42e1c634d295d307877e))

### Changed
- Clean-up and less verbose exception handling for local model update ([a943e71](https://github.com/id-data-foundry/data-foundry/commit/a943e718607df453ca46a06752f24912ec982938))
- Simplify the IoT dataset submission to a single format / API method ([8477555](https://github.com/id-data-foundry/data-foundry/commit/8477555aa1f1b901bd4b9551c2013f26f4965310))
- Knowledge improvements
system prompt improvements
notification improvements
fixed reconnection for chat ([1fb1238](https://github.com/id-data-foundry/data-foundry/commit/1fb1238a7865704e8a3f9c8cca221219f7714a7b))

### Removed
- Removed thinking traces from agent chat output
code formatting ([67bedfe](https://github.com/id-data-foundry/data-foundry/commit/67bedfe1353cf80d7af378c48e9a6f7855f9d0b7))

### Fixed
- Fixed width in coding agent layout ([84cf2e9](https://github.com/id-data-foundry/data-foundry/commit/84cf2e9a4ed2c562be605319c1764374bb2a4e2c))
- Fixed user api controller with a stable auth token
fixed profile retrieval in userJSAPI ([0ef9e83](https://github.com/id-data-foundry/data-foundry/commit/0ef9e83047a3b393b6a9a7edc9146fba41c8dd35))
- Fixed Starboard dataset import guide
Replaced pyodide.open_url for pyodide.http.open_url ([69f0bde](https://github.com/id-data-foundry/data-foundry/commit/69f0bde0c977bda585f32104ad0c04fb4c09548e))

## [v0.9.25] - 2026-06-11
### Added
- Added configuration options for local AI ([a8305ec](https://github.com/id-data-foundry/data-foundry/commit/a8305ec26a2c18892db2d78acdfd59e19887ecd5))
- Added webhook documentation ([92f60b9](https://github.com/id-data-foundry/data-foundry/commit/92f60b902ee28da628dd78d53f7769352bc061cb))
- Added webhook trigger type to scripting
UI fix: radio buttons to select ([525373d](https://github.com/id-data-foundry/data-foundry/commit/525373de213b2f395c76f1824fe37c0069a02dff))
- Added timer triggers to DF scripts ([346b15d](https://github.com/id-data-foundry/data-foundry/commit/346b15d3c385c71c4d896731be2e4fff298e77b9))
- Added workflow diagrams to scripting + iot, entity and media datasets ([776901a](https://github.com/id-data-foundry/data-foundry/commit/776901a834636c468e9a6a1c2034055f73dd0844))
- Added workflow diagram to chatbot test view ([6930211](https://github.com/id-data-foundry/data-foundry/commit/693021165c85ccb53177125a5c390c1d61eb4929))

### Changed
- Move from token cap to token throttle ([54137a3](https://github.com/id-data-foundry/data-foundry/commit/54137a3dfee83a0932bebbeaf2810f9f92653786))
- Starboard update to v0.15.8, with pyodide v0.29.3 ([2da0bbf](https://github.com/id-data-foundry/data-foundry/commit/2da0bbf62c5fa7e1dfdf460b6f3a361e2ce69c15))
- Migrate legacy OpenAI nomenclature to flexible AI/LocalAI architecture ([dd79c6e](https://github.com/id-data-foundry/data-foundry/commit/dd79c6e90042aa0163b0e938c3461f88754f81d4))
- Deleted ManagedAIApiService and removed 'openai-java' SDK dependency. ([dd79c6e](https://github.com/id-data-foundry/data-foundry/commit/dd79c6e90042aa0163b0e938c3461f88754f81d4))
- Refactored UnmanagedAIApiService to handle completions, chat, and ([dd79c6e](https://github.com/id-data-foundry/data-foundry/commit/dd79c6e90042aa0163b0e938c3461f88754f81d4))
- Switched configuration from 'localai.host' to 'ai.baseurl' with ([dd79c6e](https://github.com/id-data-foundry/data-foundry/commit/dd79c6e90042aa0163b0e938c3461f88754f81d4))
- Renamed all controller methods and routes from 'OpenAI' to generic ([dd79c6e](https://github.com/id-data-foundry/data-foundry/commit/dd79c6e90042aa0163b0e938c3461f88754f81d4))
- Implemented conditional rendering of the Project API Access UI ([dd79c6e](https://github.com/id-data-foundry/data-foundry/commit/dd79c6e90042aa0163b0e938c3461f88754f81d4))
- Surgically refactored JSActor and JSExecutorService to adopt the ([dd79c6e](https://github.com/id-data-foundry/data-foundry/commit/dd79c6e90042aa0163b0e938c3461f88754f81d4))
- Updated documentation and UI templates to reflect nomenclature ([dd79c6e](https://github.com/id-data-foundry/data-foundry/commit/dd79c6e90042aa0163b0e938c3461f88754f81d4))

### Performance
- Changed IOT dataset recording to batched recording for better performance ([7b8c828](https://github.com/id-data-foundry/data-foundry/commit/7b8c8282605396250bc0dc00812ccb636608841a))

### Removed
- Removed direct api controller because it's no longer needed ([5b21124](https://github.com/id-data-foundry/data-foundry/commit/5b21124404ee4b8d0e3085582d82cc15d1009db4))
- Removed direct OpenAI API support, model enumeration, mentions in UI, etc. ([82f6f96](https://github.com/id-data-foundry/data-foundry/commit/82f6f963f53599e92b972aeb16793b4407118510))

### Fixed
- Fixed Guice / Ebean initialization bug ([8e20161](https://github.com/id-data-foundry/data-foundry/commit/8e201618ebf68f18f6b5748f23d506748e5baab4))
- Fixed complete and media DS resets, file deletion
simplified AI API internally without managed AI api service
integrated remote api request types for path selection
integrated base url for api requests
fixed AI api invocation from DF scripts (incl. docs)
aligned streaming remote api request signature
simplified response parsing in a util method ([a06475f](https://github.com/id-data-foundry/data-foundry/commit/a06475f183ff297b8517760f524f680e113196c3))
- Fixed overlay positioning for projects.index ([a9747f8](https://github.com/id-data-foundry/data-foundry/commit/a9747f8e95ef5db757c134046b687367ebb3adde))
- Fixed comparison for ageRange and gender ([a7f7145](https://github.com/id-data-foundry/data-foundry/commit/a7f7145d370d16a4ef09bd214758a00f2abf0b69))
- Fixed configuration of session cookie TTL ([082b81c](https://github.com/id-data-foundry/data-foundry/commit/082b81c5ab752531dac23010c18e8193e0a2d918))

## [v0.9.24] - 2026-05-30
### Added
- Add time-based data filtering and fix wavy area charts in visualization ([5ced7c6](https://github.com/id-data-foundry/data-foundry/commit/5ced7c6af8b4ecc7281c5ec86d574f69abb87b18))
- Add `getItemCount()` to `Dataset` model for efficient row counting ([5ced7c6](https://github.com/id-data-foundry/data-foundry/commit/5ced7c6af8b4ecc7281c5ec86d574f69abb87b18))
- Add unit test for `Dataset.getItemCount()` functionality ([5ced7c6](https://github.com/id-data-foundry/data-foundry/commit/5ced7c6af8b4ecc7281c5ec86d574f69abb87b18))
- Added local AI feature availability flags in backend and landing page overlay ([9702f30](https://github.com/id-data-foundry/data-foundry/commit/9702f303727ee3c7c527b1efb3ff191a5648defd))

### Changed
- Introduce a "Data Range" selector (1-60 days) in the visualization view ([5ced7c6](https://github.com/id-data-foundry/data-foundry/commit/5ced7c6af8b4ecc7281c5ec86d574f69abb87b18))
- Automatically show range selector for datasets exceeding 10,000 rows ([5ced7c6](https://github.com/id-data-foundry/data-foundry/commit/5ced7c6af8b4ecc7281c5ec86d574f69abb87b18))
- Restrict data fetch requests by start timestamp to reduce browser load ([5ced7c6](https://github.com/id-data-foundry/data-foundry/commit/5ced7c6af8b4ecc7281c5ec86d574f69abb87b18))
- Enhance tooltips with source information in visualization charts ([5ced7c6](https://github.com/id-data-foundry/data-foundry/commit/5ced7c6af8b4ecc7281c5ec86d574f69abb87b18))
- Local AI features are now more tied to /model availability and responses (no response = not active), polling every minute instead of every 30 mins ([f9f5a23](https://github.com/id-data-foundry/data-foundry/commit/f9f5a237dfcebca288e0d862c277d680ff98f635))
- Media dataset API returns file names of uploaded files
fixed get item API method
fixed API docs on view page
added API docs for Python and cURL ([bbbd489](https://github.com/id-data-foundry/data-foundry/commit/bbbd489e1018a819c07df3b383d008509a2bca07))

### Fixed
- Fixed scala version for CI build ([509d5bc](https://github.com/id-data-foundry/data-foundry/commit/509d5bc86a7a5e73e774f58a099c3d11dda4fb07))
- Fix "wavy pattern" in area charts by disabling stacking and using source identifiers ([5ced7c6](https://github.com/id-data-foundry/data-foundry/commit/5ced7c6af8b4ecc7281c5ec86d574f69abb87b18))

## [v0.9.23] - 2026-05-27
### Added
- Added streaming support to local-ai JS library
fixed streaming in the server-side API wrt. API keys ([fdd47fb](https://github.com/id-data-foundry/data-foundry/commit/fdd47fbfeed626cb08ac22742f7cd88934198eab))
- Added direct / deep link to the Local AI examples on the data tools page ([5d38f80](https://github.com/id-data-foundry/data-foundry/commit/5d38f809a57e0e06517d0e190a902355d5946cc7))
- Add client-side audio preprocessing using Mediabunny ([c770253](https://github.com/id-data-foundry/data-foundry/commit/c7702531e4e87ceb44641e7fc9a3f0612af967fc))
- Add support for audio files (mp3, wav, m4a, ogg) in Media dataset ([9fe3b88](https://github.com/id-data-foundry/data-foundry/commit/9fe3b889b5490c3db6b0991abcde3831662ca00e))

### Changed
- Extracts and transcodes audio to 16kHz MP3 before upload ([c770253](https://github.com/id-data-foundry/data-foundry/commit/c7702531e4e87ceb44641e7fc9a3f0612af967fc))
- Includes @mediabunny/mp3-encoder extension for browser compatibility ([c770253](https://github.com/id-data-foundry/data-foundry/commit/c7702531e4e87ceb44641e7fc9a3f0612af967fc))
- Uses BufferTarget to ensure valid file headers ([c770253](https://github.com/id-data-foundry/data-foundry/commit/c7702531e4e87ceb44641e7fc9a3f0612af967fc))

### Fixed
- Fixed prompt rebuilding in situations where there is just an initial (welcome) prompt and no actual user history ([4e8e5e3](https://github.com/id-data-foundry/data-foundry/commit/4e8e5e35936bb45612930c44630a8ac4d49699c1))

## [v0.9.22] - 2026-05-16
### Added
- Added documentation for the log CSV function in TimeseriesDS ([5612b0d](https://github.com/id-data-foundry/data-foundry/commit/5612b0df893eb2eb6936331b6a6b539aaa8ca839))
- Added queueing to the transcribe controller
added results view with editing and download ([b613935](https://github.com/id-data-foundry/data-foundry/commit/b6139356c5b173e57ff197bde2b1cd3e7ba84205))
- Added selective delete by time interval for data in datasets ([d9db9bd](https://github.com/id-data-foundry/data-foundry/commit/d9db9bd8f2dba859effa24ea61e5b844523d8c8a))
- Added generic (simple) throttling for controller actions ([49476e3](https://github.com/id-data-foundry/data-foundry/commit/49476e3b2bc48bed3e2843f04f3507845964cde1))
- Added playback for transcribed segments ([1090ac3](https://github.com/id-data-foundry/data-foundry/commit/1090ac355296a4c73389ae9c02a652bf32e1dc07))
- Added editing stage for timestamped transcription results ([e9fc25f](https://github.com/id-data-foundry/data-foundry/commit/e9fc25fdea3da6d68d916a1f486b16a67da40468))

### Changed
- Enhanced user profile view
added api key overview ([475ff21](https://github.com/id-data-foundry/data-foundry/commit/475ff21926cb4ff8f04220c1fa16f817973199bc))
- Local AI library update ([989ade0](https://github.com/id-data-foundry/data-foundry/commit/989ade087c82fd11fa44b4a55c8f6dedc131923e))
- Improved transcription view for diarization ([6a815e0](https://github.com/id-data-foundry/data-foundry/commit/6a815e027c51923e2eaf69ae536ce4a4d6be9e2d))
- Transcription view updates (data-backed model selection, SRT format output, downloads, automatic AI statements)
improved local-ai JS library for better speech-to-text functionality (frontend VAD)
documentation updates ([e841dd3](https://github.com/id-data-foundry/data-foundry/commit/e841dd38e3dfa55b661fd30fd082337c8a9e7260))
- Improved layout and interaction flow in transcription view ([c9873cd](https://github.com/id-data-foundry/data-foundry/commit/c9873cd955594f5b2a245c211a686204c945198c))

### Fixed
- Fixed dependency inclusion for the mobile chart view ([2e63093](https://github.com/id-data-foundry/data-foundry/commit/2e6309386cf24039f2d61e3fbfd2a9d429aae88c))
- Fixed selectDeleteByTime form submission ([1fb35f0](https://github.com/id-data-foundry/data-foundry/commit/1fb35f0542aac96665711d00387c2ae20120ae19))

## [v0.9.21] - 2026-05-01
### Added
- Added usage info variants to some local AI functions ([3fbace9](https://github.com/id-data-foundry/data-foundry/commit/3fbace920371e55dad91d092b0a8e6ea0a1acc9f))
- Added selective delete to dataset edit screens ([dc71ff8](https://github.com/id-data-foundry/data-foundry/commit/dc71ff8443a4c67d9e6c7657987d2d5f8a6a708e))

### Changed
- Updated documentation ([5d8d7f3](https://github.com/id-data-foundry/data-foundry/commit/5d8d7f3aefa4197960d902217d198f0e6d51194e))
- Further clean-up of flashes.js ([044c1c4](https://github.com/id-data-foundry/data-foundry/commit/044c1c438dd947918cd5db86bfeca19f89c34590))
- Request handling improvements unmanaged AI service ([8599edf](https://github.com/id-data-foundry/data-foundry/commit/8599edf65e51ecbcf9252c89a9acd9ecf2cc9168))
- Revert unnecessary change ([d8e415e](https://github.com/id-data-foundry/data-foundry/commit/d8e415ebc0cd69c384f25d8c65cd7d1a5c6b0d17))
- Improved API documentation for transcribe and PDF text extraction ([83cafa6](https://github.com/id-data-foundry/data-foundry/commit/83cafa6f213731bed9f2e91be2b0b26914ec5a5b))

### Fixed
- Fix for driverjs, where it seems a bracket issue caused an
addeventlistener error. Needs extra inspection because of AI ([cf1b4b6](https://github.com/id-data-foundry/data-foundry/commit/cf1b4b62a2e427a2d2e3730d16414bfb1cd2a819))
- Fixed API key check in pdfToText API
documentation update ([fa024ed](https://github.com/id-data-foundry/data-foundry/commit/fa024ed811842699368b82238c1d8824ad7fde80))
- Fixed potential sql injection vectors ([ba433ee](https://github.com/id-data-foundry/data-foundry/commit/ba433eedc4b4ccb9dfee3d2b4b97e12413ace87e))

## [v0.9.20] - 2026-04-27
### Added
- Added timestamps UI elements ([804c01d](https://github.com/id-data-foundry/data-foundry/commit/804c01dda7899790c173d82333661a6654cdaa1d))
- Added Local AI-supported translation tool ([eea465e](https://github.com/id-data-foundry/data-foundry/commit/eea465e054aca1b5b705740b4e718ac828ea80cf))
- Added file upload to chatbot API
added API documentation at the bottom ([e475f26](https://github.com/id-data-foundry/data-foundry/commit/e475f26cf94e8b450daa183ce49926eb20b53753))
- Added download button to visualization ([ee81c3b](https://github.com/id-data-foundry/data-foundry/commit/ee81c3bee3cfd767b4533d7687a27bb6e3fb070b))
- Added download and refresh buttons to the activity logger ([c58ae5d](https://github.com/id-data-foundry/data-foundry/commit/c58ae5d3bd069eb18d8de2c2020f4944e02cf63a))
- Added image annotation to modal dialog
fixed tooltip table formatting ([79647b1](https://github.com/id-data-foundry/data-foundry/commit/79647b1fde76bdd5694eac23f011282415c1b2a9))
- Added image display for media datasets ([569d1f7](https://github.com/id-data-foundry/data-foundry/commit/569d1f7ccdd81ce4efee68115f611dee17782f31))
- Added dimensions to the exclusion list for preprocessing ([461041b](https://github.com/id-data-foundry/data-foundry/commit/461041bc9b4a28c88e17820f16dcb9f4f625c64f))

### Changed
- Refactored media to text processing internals ([bf51852](https://github.com/id-data-foundry/data-foundry/commit/bf51852633af4fed78b564f57aa5b54597d99dd5))
- Restructured data tools screen
added ai tools section that is shown only with AI configured and greyed out when temporarily not available
removed DF boost and export tool ([2e72d92](https://github.com/id-data-foundry/data-foundry/commit/2e72d9217f970fa80091c35a2717dbc2d0ab9193))
- Enabled chatbot knowledge base handling of text and markdown files ([ff307dd](https://github.com/id-data-foundry/data-foundry/commit/ff307dd72ff4978a63743de38f62a15edbe9a63a))
- Simplified the dataset/dimension display for datasets with only a single dimension ([6645b0f](https://github.com/id-data-foundry/data-foundry/commit/6645b0ff00c263c25654eadb4e07d10d4f032e57))
- Aligned cluster management view with other source management views
removed old code (manageResources)
move cluster management in sources tab to the bottom ([238a417](https://github.com/id-data-foundry/data-foundry/commit/238a417fb4d38559539c3c12face60cf27efe708))
- Consolidated integrated data visualizations for devices and participants towards the project data vis perspective
streamlined device and participant views
removed unused code
restructured dataset downloads
improved cluster filtering for dataset downloads ([636b8be](https://github.com/id-data-foundry/data-foundry/commit/636b8bee4d6c05c84989b083ebbd96e66abc6cd0))
- Unified the visualization tool ([38afb4c](https://github.com/id-data-foundry/data-foundry/commit/38afb4c3f55519e54106cf9edcbc4c2f8a7c09ee))
- Modal window for image preview ([1a2cf84](https://github.com/id-data-foundry/data-foundry/commit/1a2cf84855ddc1b6d400dc8cb8165fa672595273))
- Refinements for annotation and diary dataset types, first draft media dataset ([f8e9750](https://github.com/id-data-foundry/data-foundry/commit/f8e9750e424d5ea95d752af47ecc994259ea2eab))
- Further improved layout of visualizations
fixed buttons
added select icon ([ab88987](https://github.com/id-data-foundry/data-foundry/commit/ab889872e5bb97b8bec7960a0761eff48e6c3124))
- Refined the visualization layout ([859e2b4](https://github.com/id-data-foundry/data-foundry/commit/859e2b461a499df5c626f51636cbeac4a645152d))
- Improved data logger output for buttons ([f54b1f8](https://github.com/id-data-foundry/data-foundry/commit/f54b1f8c6d27dab40040cf31748ca543532a8e28))
- Aligned edit button for script datasets
removed error messages after new login ([826839f](https://github.com/id-data-foundry/data-foundry/commit/826839f1532fb950bbbb5e88b219a862ef515679))
- First draft new visualization tool ([7c35913](https://github.com/id-data-foundry/data-foundry/commit/7c3591366e354637688bbb5225062135912934e8))

### Fixed
- Fixed tooltips
consolidated all vis libraries in new visualization ([7039877](https://github.com/id-data-foundry/data-foundry/commit/703987703917cac700bbb8c1c76953a4637e26a8))
- Fixed nav breadcrumbs
fixed CSS for transcription page
removed busyness indicator ([5f50fad](https://github.com/id-data-foundry/data-foundry/commit/5f50fadfd8ee6f4259fb7c944df3cd72317ece30))
- Fix URL decoding across several controllers and models ([a4c2a11](https://github.com/id-data-foundry/data-foundry/commit/a4c2a11a3d5b682eb432ed2e3d7ac585c5f5841f))
- Fixed FAQ link ([1cc858b](https://github.com/id-data-foundry/data-foundry/commit/1cc858b7d30884e1de5e58e0ef48a5028486c6f9))

## [v0.9.19] - 2026-04-24
### Added
- Added image edit API call and route for media dataset
added image edit screen
fixed image retrieval ([421a24a](https://github.com/id-data-foundry/data-foundry/commit/421a24a64d4e69b8f84055a4ac66faeba14d0c91))
- Added device info ([3386ecf](https://github.com/id-data-foundry/data-foundry/commit/3386ecfc34d6c7147fae1de24b1e22a71450454a))

### Changed
- Update Making Sense of Sensors tour ([3386ecf](https://github.com/id-data-foundry/data-foundry/commit/3386ecfc34d6c7147fae1de24b1e22a71450454a))
- Streamlined code ([3386ecf](https://github.com/id-data-foundry/data-foundry/commit/3386ecfc34d6c7147fae1de24b1e22a71450454a))
- Changed text ([3386ecf](https://github.com/id-data-foundry/data-foundry/commit/3386ecfc34d6c7147fae1de24b1e22a71450454a))
- Enabled public projects (organization-wide) ([3f982b3](https://github.com/id-data-foundry/data-foundry/commit/3f982b3ad9a770945c722292124819782f8a77c3))
- MSS course template and ViperIDE updates ([94be41c](https://github.com/id-data-foundry/data-foundry/commit/94be41c614d62968faf248fd24da27fe46eb6d63))
- Downgrade micropython to 1.27.0 ([991f38a](https://github.com/id-data-foundry/data-foundry/commit/991f38a4e29817b541adb346b125c1b2027317bc))
- 1.28.0 doesnt have working ssl ([991f38a](https://github.com/id-data-foundry/data-foundry/commit/991f38a4e29817b541adb346b125c1b2027317bc))
- Micorpython version increase ([878507d](https://github.com/id-data-foundry/data-foundry/commit/878507df22e26bf30d9ad82c48cc1423c98e7fee))
- Changed some making sense of sensor related text ([878507d](https://github.com/id-data-foundry/data-foundry/commit/878507df22e26bf30d9ad82c48cc1423c98e7fee))
- Tested on esp32-c6 and esp32-s3 ([878507d](https://github.com/id-data-foundry/data-foundry/commit/878507df22e26bf30d9ad82c48cc1423c98e7fee))
- Msos tweaks ([d6a3723](https://github.com/id-data-foundry/data-foundry/commit/d6a3723778e04ba0247838afe75f077e63bc4d93))
- Updated micropython to V1.28, added esp32-c6 support ([75150db](https://github.com/id-data-foundry/data-foundry/commit/75150db8aef675a5b7e83ffbb1161a73b9df9d19))

### Removed
- Removed external public project search
cleaned up project card for archived projects
fixed wording for public project settings
clarified wording for search page ([9e47f13](https://github.com/id-data-foundry/data-foundry/commit/9e47f1333808c268dd62bf3622f2646c62d2e577))
- Removed micropython v1.24.0 as it's not needed / linked anymore ([9d0ca91](https://github.com/id-data-foundry/data-foundry/commit/9d0ca91875ce51f13b07e5f5480fbae30d5e46ce))

### Fixed
- Fixed QR code generator for query parameters ([d302cb1](https://github.com/id-data-foundry/data-foundry/commit/d302cb1e9efbbd7c5f8d91652f95893373430a46))
- Fixed documentation link patcher
Split documentation links into DOMAIN and URL
Update to latest documentation version
Fixed small typo in documentation tour ([da14a67](https://github.com/id-data-foundry/data-foundry/commit/da14a67aa44a8e65c9a6b6d9b80711fa99605644))
- Fixed project template MSS part 2 ([18899a1](https://github.com/id-data-foundry/data-foundry/commit/18899a1277ff9b8116d36be0ba56a9db1fc0719b))
- Fixed QR code generator for URLs with query parameters ([566a33c](https://github.com/id-data-foundry/data-foundry/commit/566a33c39ee07ff5f2886b1c69b80bf1dcd3f12b))
- Fixed broken step on tools page ([3386ecf](https://github.com/id-data-foundry/data-foundry/commit/3386ecfc34d6c7147fae1de24b1e22a71450454a))
- Fixed offline operation bug for OOCSI clients ([acda5b1](https://github.com/id-data-foundry/data-foundry/commit/acda5b1f832ac4b8408be29ffa3eb43a94e07b72))

## [v0.9.18] - 2026-04-18
### Added
- Added COOP/COEP headers for Starboard edit notebook route, solving potential resource loading / webworker issues for Starboards ([5df1d9d](https://github.com/id-data-foundry/data-foundry/commit/5df1d9d524b36b61c4bfd029fed6bc43972f9fc6))
- Added API route and controller action to process PDF-to-text conversions via the API ([ad22d9c](https://github.com/id-data-foundry/data-foundry/commit/ad22d9ce2f49b0d81544bb2c5da650f3e8ebd60d))

### Changed
- Updated PDF processing dependency ([d3e5d5d](https://github.com/id-data-foundry/data-foundry/commit/d3e5d5dbbac35ba4999846bda23d64a025786dba))
- Switched media processing controller to Local AI backend for audio and video requests, PDFs stay with media processing service ([d3e5d5d](https://github.com/id-data-foundry/data-foundry/commit/d3e5d5dbbac35ba4999846bda23d64a025786dba))
- Extended the info that goes with media processing calls to model, timestamps, diarization, potentially multiple files ([d3e5d5d](https://github.com/id-data-foundry/data-foundry/commit/d3e5d5dbbac35ba4999846bda23d64a025786dba))
- Switched frontend from jQuery to vanilla JS ([d3e5d5d](https://github.com/id-data-foundry/data-foundry/commit/d3e5d5dbbac35ba4999846bda23d64a025786dba))
- Moved qr code route to QRCode ([e940d13](https://github.com/id-data-foundry/data-foundry/commit/e940d13095e1f77080d244dac7c93c75963566cf))

### Removed
- Removed unused code ([d3e5d5d](https://github.com/id-data-foundry/data-foundry/commit/d3e5d5dbbac35ba4999846bda23d64a025786dba))

### Fixed
- Fixed file handling for PDF processing, added markdown image tag filter ([d3e5d5d](https://github.com/id-data-foundry/data-foundry/commit/d3e5d5dbbac35ba4999846bda23d64a025786dba))
- Fixed documentation ([d3e5d5d](https://github.com/id-data-foundry/data-foundry/commit/d3e5d5dbbac35ba4999846bda23d64a025786dba))

## [v0.9.17] - 2026-03-22
### Added
- Added warnings in code and ui when a dataset is selection without participants and without "open participation"
added comments for activity logger
added possibility to turn adhoc activities into buttons
added different data vis for comments (than other events/states)
added sorting in the data vis, so comments are always at bottom ([51c9a4e](https://github.com/id-data-foundry/data-foundry/commit/51c9a4e876f6dc490cfa23204ffdac110549807b))
- Added source representation for chatbots, on demand ([50d37f7](https://github.com/id-data-foundry/data-foundry/commit/50d37f7ddcebcf24558cd76aeb44fc9abebc4aa7))

### Changed
- Unified number parsing in internal util, which is then made more robust through apache NumberUtils ([83ceb80](https://github.com/id-data-foundry/data-foundry/commit/83ceb80748eafc1a57bd0fc6bf7e82a45ed4d4e9))

## [v0.9.16] - 2026-03-15
### Added
- Added sbt to ci.yml ([e1b58a1](https://github.com/id-data-foundry/data-foundry/commit/e1b58a1190d34c447b6d24e215f6453f14141b22))

### Fixed
- Fixed badge in readme, added ci.yml for running tests ([bb49a0a](https://github.com/id-data-foundry/data-foundry/commit/bb49a0a8f3bdb63f4478c06108da1f6c7b4b8d9a))

## [v0.9.15] - 2026-02-16
### Added
- Added missing library files ([83d47c8](https://github.com/id-data-foundry/data-foundry/commit/83d47c82edef8ed4eb5d025461e395eba0efe976))

### Changed
- Included submodule in checkout ([bece64f](https://github.com/id-data-foundry/data-foundry/commit/bece64f4c82a83feb621dc5d9623c2eb8158c542))
- Self-contained Dockerfile for all build steps ([a899014](https://github.com/id-data-foundry/data-foundry/commit/a899014b9f1cfe83b1b3dcce9409a61ba18ed94a))
- Update Docker publish workflow to include tags

Integrated proposal from #1 ([f89633d](https://github.com/id-data-foundry/data-foundry/commit/f89633d6510a6af4511b630ff4c9f3886526ed24))
- Development:
  github actions
  additional deployment examples
  Open API / Swagger
  common module
  deployment examples
  distribution files
  application configuration
  devcontainers
  project setup
  git setup ([98b349f](https://github.com/id-data-foundry/data-foundry/commit/98b349fd64212f5341abfc1e039e476f9072aaee))
- Github actions ([9d18ba3](https://github.com/id-data-foundry/data-foundry/commit/9d18ba35216e8fdabb16b05699f0010d249c2899))
- Additional deployment examples ([e74b623](https://github.com/id-data-foundry/data-foundry/commit/e74b623d8b13b365044789e75d0cb46dbbd03ceb))
- Open API / Swagger ([e66537b](https://github.com/id-data-foundry/data-foundry/commit/e66537b61ab931181b22cf12872fa4f99657ee21))
- Common module ([8c4d85f](https://github.com/id-data-foundry/data-foundry/commit/8c4d85f215ce9a2c8d238d7bf69f8739154d7321))
- Deployment examples ([2f8b767](https://github.com/id-data-foundry/data-foundry/commit/2f8b7678794c2b718b7c5adb2e8c9e9f77d6e44f))
- Distribution files ([1752335](https://github.com/id-data-foundry/data-foundry/commit/17523352e29af03a945e93115ba1251f858185b0))
- Application configuration ([12312f4](https://github.com/id-data-foundry/data-foundry/commit/12312f43c42b2dc10f6f8804e76346b0d5b9aa9e))
- Devcontainers ([809b27d](https://github.com/id-data-foundry/data-foundry/commit/809b27dfc880d6e143b74f25865701f2a967baed))
- Project setup ([122895a](https://github.com/id-data-foundry/data-foundry/commit/122895a6e82812600b1aa01f15fe75df3909e151))
- Git setup ([4094181](https://github.com/id-data-foundry/data-foundry/commit/4094181c9fe5a5dc4908553cc407feb019241b3f))

### Fixed
- Fixed versioning metadata 2 ([cb5b699](https://github.com/id-data-foundry/data-foundry/commit/cb5b699d9746477454b244e72a0adfb6584f0430))
- Fixed versioning metadata ([dbd9596](https://github.com/id-data-foundry/data-foundry/commit/dbd95969ea706629543fa08ed9e1ea9c422544bb))
- Fixed permissions with a GH token ([345730e](https://github.com/id-data-foundry/data-foundry/commit/345730ec520120c2652aec008f0573046c3eacd1))
- Fixed indentation ([bb93d8e](https://github.com/id-data-foundry/data-foundry/commit/bb93d8ec101820e5ffe0e8404f1502a376196d15))
- Fix Dockerfile path in docker-publish workflow ([00298bc](https://github.com/id-data-foundry/data-foundry/commit/00298bc7dea527fa3213550aaa74c1452868dbd9))

## [v0.9.14] - 2026-02-08
### Security
- Removed CSS security service, not needed ([6c87750](https://gitlab.tue.nl/id-dept/id-projects/-/commit/6c877501c6989debc5a7e52819b1c85c6430615a))

### Added
- Add org-specific contact ([f227d72](https://gitlab.tue.nl/id-dept/id-projects/-/commit/f227d72e352b25708509ce0906fd16fca01fe5e2))
- Added placeholder to standard.css ([16ee499](https://gitlab.tue.nl/id-dept/id-projects/-/commit/16ee4996ff2d4f97280d8ae09d6bcda5f6735f73))
- Added structure to project listings ([6358fee](https://gitlab.tue.nl/id-dept/id-projects/-/commit/6358fee968fcd97c2177505b6a991d2a72aaaf1c))
- Added wording to introduce the media browser ([e40e477](https://gitlab.tue.nl/id-dept/id-projects/-/commit/e40e47758475948f07f8522456d972d222b0276c))
- Added minimal dark template ([71f33d7](https://gitlab.tue.nl/id-dept/id-projects/-/commit/71f33d7d27941ab376d60450fb688723dbf20fb4))
- Added minimal dark template ([efd5a9f](https://gitlab.tue.nl/id-dept/id-projects/-/commit/efd5a9f29f89ef667976ec6b4e44cccd8265b74f))
- Added second open page button ([de07c98](https://gitlab.tue.nl/id-dept/id-projects/-/commit/de07c98b2cc7ca24a05ce901b43fa9b1f0fb1c89))
- Added CSS template comments ([6131161](https://gitlab.tue.nl/id-dept/id-projects/-/commit/6131161b37a285d0b13ced8c871ea2d4b2b543f9))
- Added second save button ([355ea9c](https://gitlab.tue.nl/id-dept/id-projects/-/commit/355ea9c2c1115c6733564a0f6b9cf5abc88f1d00))
- Added minimal template ([8870dc9](https://gitlab.tue.nl/id-dept/id-projects/-/commit/8870dc92186dd77a45b96db9c007fe419ae93eea))
- Added two new CSS tempaltes ([e0bb58a](https://gitlab.tue.nl/id-dept/id-projects/-/commit/e0bb58af62955b4fe98cfe29db0885a170047c4c))
- Added bladerunner and pastel templates ([de9a5a9](https://gitlab.tue.nl/id-dept/id-projects/-/commit/de9a5a911758d0518224919d0eaf727214ebe41a))
- Added dark mode portfolio template ([eecf2fc](https://gitlab.tue.nl/id-dept/id-projects/-/commit/eecf2fc509a2109d06aad4edd97e1672149f45d2))
- Added showtime button to collections ([581d0af](https://gitlab.tue.nl/id-dept/id-projects/-/commit/581d0af93f467b858cb2fab23c874642c29445ad))
- Added kiosk exit at the end of the show for pechakucha mode ([d68b408](https://gitlab.tue.nl/id-dept/id-projects/-/commit/d68b408849579a704f21a268598d1863120ac797))
- Added pechakucha mode to the showtime feature ([813ccf7](https://gitlab.tue.nl/id-dept/id-projects/-/commit/813ccf7d0acad5a7ff931c418ac314344576f590))
- Added project tags ([8cacde6](https://gitlab.tue.nl/id-dept/id-projects/-/commit/8cacde62b2a71ab4a9833a6a77caa803c801a304))
- Added word counter on portfolio page ([9b2a3bd](https://gitlab.tue.nl/id-dept/id-projects/-/commit/9b2a3bd575dce0867d0bfafe217fd21e4922ce85))
- Added media browsing styles correctly ([5de26a0](https://gitlab.tue.nl/id-dept/id-projects/-/commit/5de26a0a7825a16c7d56c0bfb50acf69f7ca21e6))
- Added current row estimation ([5de26a0](https://gitlab.tue.nl/id-dept/id-projects/-/commit/5de26a0a7825a16c7d56c0bfb50acf69f7ca21e6))
- Added AI statement to end of generated portfolio ([653cb11](https://gitlab.tue.nl/id-dept/id-projects/-/commit/653cb11864304a36615aa81be03354c0e2b6a052))
- Added image link to project embedding ([d9f2336](https://gitlab.tue.nl/id-dept/id-projects/-/commit/d9f23366cd8a8ff9cadb60e22b0e4eb69c92cbbc))
- Added next line (paragraph) to the project  embedding to allow for reflection paragraphs ([d9f2336](https://gitlab.tue.nl/id-dept/id-projects/-/commit/d9f23366cd8a8ff9cadb60e22b0e4eb69c92cbbc))

### Changed
- Styling improvements portfolio page ([16ee499](https://gitlab.tue.nl/id-dept/id-projects/-/commit/16ee4996ff2d4f97280d8ae09d6bcda5f6735f73))
- Explained image browser in mini docs on page ([e40e477](https://gitlab.tue.nl/id-dept/id-projects/-/commit/e40e47758475948f07f8522456d972d222b0276c))
- Updated the PDF download ([e40e477](https://gitlab.tue.nl/id-dept/id-projects/-/commit/e40e47758475948f07f8522456d972d222b0276c))
- Adapted the other templates as well ([0571f1f](https://gitlab.tue.nl/id-dept/id-projects/-/commit/0571f1fc697acab587dc63e2b4d15ef05c24ad5b))
- Rearranged template order ([54d95ee](https://gitlab.tue.nl/id-dept/id-projects/-/commit/54d95eeca7df45beabbfdc1a814a5445832d5445))
- 'portfolio-feature' of https://gitlab.tue.nl/id-dept/id-projects: ([71f33d7](https://gitlab.tue.nl/id-dept/id-projects/-/commit/71f33d7d27941ab376d60450fb688723dbf20fb4))
- Updated the HTML project rendering, which allows for more placement freedom in CSS ([748613b](https://gitlab.tue.nl/id-dept/id-projects/-/commit/748613b61027666dfc09cbfc793a59ef5a9b0ae6))
- Moved and arranged second save button on portfolio page ([de07c98](https://gitlab.tue.nl/id-dept/id-projects/-/commit/de07c98b2cc7ca24a05ce901b43fa9b1f0fb1c89))
- Ensured owner header height is 2rem ([de07c98](https://gitlab.tue.nl/id-dept/id-projects/-/commit/de07c98b2cc7ca24a05ce901b43fa9b1f0fb1c89))
- Changed custom CSS frontend selection behavior ([34d9c0e](https://gitlab.tue.nl/id-dept/id-projects/-/commit/34d9c0ef27883704f4c7e7da47847e0ed198b15a))
- Rendering of template CSS now when the a template is selected ([0ee21c4](https://gitlab.tue.nl/id-dept/id-projects/-/commit/0ee21c47af5e5bd1a9866b2133abbb61c7a901d2))
- Made the onUpdate behavior less aggressive in editMarkdown css style editor ([a2c5671](https://gitlab.tue.nl/id-dept/id-projects/-/commit/a2c56713761237f70b3a978b886bc47af8790930))
- Enhanced the bladerunner theme ([741f9f6](https://gitlab.tue.nl/id-dept/id-projects/-/commit/741f9f63a3ec2de54efc8074848cc0b7e6a16ee9))
- CSS template fixes ([0ec9c1d](https://gitlab.tue.nl/id-dept/id-projects/-/commit/0ec9c1d050f9aadf3a7c740840b0c0d1bd1f3d3e))
- CSS fixes for both themes ([95b6e8d](https://gitlab.tue.nl/id-dept/id-projects/-/commit/95b6e8d176e324ae9d11fb84b0fac8d0fdcb17bd))
- Rebuilt the CSS template structure for more extensibility ([71677f6](https://gitlab.tue.nl/id-dept/id-projects/-/commit/71677f61036630a8ceb8ffb013d153ea49d3089c))
- Changed button styles for the showtime buttons ([581d0af](https://gitlab.tue.nl/id-dept/id-projects/-/commit/581d0af93f467b858cb2fab23c874642c29445ad))
- Progress bar in showtime footer ([a66446e](https://gitlab.tue.nl/id-dept/id-projects/-/commit/a66446e4fd4600566e77bae393e8aeba9ce6942d))
- Revised project embedding ([8cacde6](https://gitlab.tue.nl/id-dept/id-projects/-/commit/8cacde62b2a71ab4a9833a6a77caa803c801a304))
- Integrated style editor in the markdown component ([390e73d](https://gitlab.tue.nl/id-dept/id-projects/-/commit/390e73db0732d84016212bed5a6b8274d2234b6c))
- Layout restructuring of portfolio edit page ([390e73d](https://gitlab.tue.nl/id-dept/id-projects/-/commit/390e73db0732d84016212bed5a6b8274d2234b6c))
- Rolled back styling to original ([390e73d](https://gitlab.tue.nl/id-dept/id-projects/-/commit/390e73db0732d84016212bed5a6b8274d2234b6c))
- Smaller fixes ([390e73d](https://gitlab.tue.nl/id-dept/id-projects/-/commit/390e73db0732d84016212bed5a6b8274d2234b6c))
- First draft portfolio styling ([10e73c0](https://gitlab.tue.nl/id-dept/id-projects/-/commit/10e73c0ccb1fb7f871f4b5b803c107b6ee8913d7))
- Code formatting ([5de26a0](https://gitlab.tue.nl/id-dept/id-projects/-/commit/5de26a0a7825a16c7d56c0bfb50acf69f7ca21e6))
- Minor tweaks to styling of media grid (hover fx) ([5de26a0](https://gitlab.tue.nl/id-dept/id-projects/-/commit/5de26a0a7825a16c7d56c0bfb50acf69f7ca21e6))
- Media selection as detail/summary ([5de26a0](https://gitlab.tue.nl/id-dept/id-projects/-/commit/5de26a0a7825a16c7d56c0bfb50acf69f7ca21e6))
- First draft media management ([5235fc7](https://gitlab.tue.nl/id-dept/id-projects/-/commit/5235fc71f45093bb639d9f46b700b3f412d80808))
- Promote the new syntax with reflection on the next line ([4a2aa13](https://gitlab.tue.nl/id-dept/id-projects/-/commit/4a2aa13136092ff943151b3fc76d204c145fe4c4))
- Extract preprocessing method and used that earlier in the flow, before the markdown is parsed ([890d309](https://gitlab.tue.nl/id-dept/id-projects/-/commit/890d3099092ad987e4d307522b4685347be89764))
- First draft portfolio feature ([3c1bfa6](https://gitlab.tue.nl/id-dept/id-projects/-/commit/3c1bfa68c142d14d1f87b3e1629ff9799a50e843))
- Implementation plans ([e94ba47](https://gitlab.tue.nl/id-dept/id-projects/-/commit/e94ba473f85e2f22e0b8e44692c92a9bc49f7da5))

### Removed
- Removed name from header because it's sometimes rendered wrong ([60befc6](https://gitlab.tue.nl/id-dept/id-projects/-/commit/60befc6f0c122eec1bb1ba510f2ef572e9025aad))
- Removed link to person in showtime views ([581d0af](https://gitlab.tue.nl/id-dept/id-projects/-/commit/581d0af93f467b858cb2fab23c874642c29445ad))
- Removed plans ([c2350e3](https://gitlab.tue.nl/id-dept/id-projects/-/commit/c2350e32b379f427a957b1f69ac9bbf8b3429f5d))
- Removed old syntax entirely ([890d309](https://gitlab.tue.nl/id-dept/id-projects/-/commit/890d3099092ad987e4d307522b4685347be89764))

### Fixed
- Fixed portfolio renderer on first generation, and template component ([16ee499](https://gitlab.tue.nl/id-dept/id-projects/-/commit/16ee4996ff2d4f97280d8ae09d6bcda5f6735f73))
- Fixed wording and docs on frontend ([16ee499](https://gitlab.tue.nl/id-dept/id-projects/-/commit/16ee4996ff2d4f97280d8ae09d6bcda5f6735f73))
- Fixed script errors by enclosing them as modules ([16ee499](https://gitlab.tue.nl/id-dept/id-projects/-/commit/16ee4996ff2d4f97280d8ae09d6bcda5f6735f73))
- Fixed classess on hero section ([950d739](https://gitlab.tue.nl/id-dept/id-projects/-/commit/950d739fee8802af75714f0e597302b514c2ec1c))
- Fixed template backgrounds and header height ([0b61576](https://gitlab.tue.nl/id-dept/id-projects/-/commit/0b61576f56dbbd066c3af54850edb33cf1b2a3bb))
- Fixed fonts in the markdown and css editors ([fce1b39](https://gitlab.tue.nl/id-dept/id-projects/-/commit/fce1b39e847c66015a3a7a132eef2dddce2ab157))
- Fixed owner header ([748613b](https://gitlab.tue.nl/id-dept/id-projects/-/commit/748613b61027666dfc09cbfc793a59ef5a9b0ae6))
- Fixed default CSS for the new HTML ([748613b](https://gitlab.tue.nl/id-dept/id-projects/-/commit/748613b61027666dfc09cbfc793a59ef5a9b0ae6))
- Fixed docs for portfolio page ([a4a7651](https://gitlab.tue.nl/id-dept/id-projects/-/commit/a4a76519eb0e002c6a2224e103cb57ace35aceb3))
- Fixed selection box for custom styles ([2e994d5](https://gitlab.tue.nl/id-dept/id-projects/-/commit/2e994d55200e215c10862fb39e3cf885ca4ae8d1))
- Fixed initial implementation ([71d9d25](https://gitlab.tue.nl/id-dept/id-projects/-/commit/71d9d25940237688d64a8541417ef7c131fc4373))
- Fixed wording ([2e28ad8](https://gitlab.tue.nl/id-dept/id-projects/-/commit/2e28ad8259089dee9d102d2025b24bb06f72ddf3))
- Fixed file chunk duplication issue ([6c87750](https://gitlab.tue.nl/id-dept/id-projects/-/commit/6c877501c6989debc5a7e52819b1c85c6430615a))
- Fixed document element selection bug ([5de26a0](https://gitlab.tue.nl/id-dept/id-projects/-/commit/5de26a0a7825a16c7d56c0bfb50acf69f7ca21e6))
- Fixed project cover/image retrieval for embedding ([653cb11](https://gitlab.tue.nl/id-dept/id-projects/-/commit/653cb11864304a36615aa81be03354c0e2b6a052))

## [v0.9.13] - 2026-01-30
### Added
- Added project modal to all applicable collections ([776f535](https://gitlab.tue.nl/id-dept/id-projects/-/commit/776f535ab238f3bf2051ec16254e35c63fa3520a))
- Added element ([c5a814d](https://gitlab.tue.nl/id-dept/id-projects/-/commit/c5a814da912e89ac1222ecbaa77aa2f68b9c4995))
- Added modal for new project via categories ([fc1e3f5](https://gitlab.tue.nl/id-dept/id-projects/-/commit/fc1e3f50a4c06856d760c2a7ebb66adb1bd52567))
- Added new project for squad, course, and collections, forwards to editProject eventually ([dcfedae](https://gitlab.tue.nl/id-dept/id-projects/-/commit/dcfedae296394ce883cb04490601296d517da9fc))

### Changed
- Better placement of showtime buttons and new buttons in categories ([52b7804](https://gitlab.tue.nl/id-dept/id-projects/-/commit/52b78049dfecc55ba03f9945adb12fa07eb85576))
- Adopted new documentation ([c37ae4d](https://gitlab.tue.nl/id-dept/id-projects/-/commit/c37ae4d260e45d40b4b87f456cae3169a6800d26))
- Changed build to work without symlink on modules/common ([ba171e6](https://gitlab.tue.nl/id-dept/id-projects/-/commit/ba171e62650de91ba6f70d4f94f71476af735c7b))
- Renamed kiosk to showtime ([4932b7c](https://gitlab.tue.nl/id-dept/id-projects/-/commit/4932b7ca76c949c80292ebca818fa804bac4f316))
- Shortened POST route for new project in categories controller ([4932b7c](https://gitlab.tue.nl/id-dept/id-projects/-/commit/4932b7ca76c949c80292ebca818fa804bac4f316))
- Semester to the showtime selection ([5e3d6be](https://gitlab.tue.nl/id-dept/id-projects/-/commit/5e3d6be5a31b66da576f67190860fcb6a2658f87))

### Removed
- Removed submodule documentation, part 2 ([8c18520](https://gitlab.tue.nl/id-dept/id-projects/-/commit/8c1852021a001c4a4c548c6617c8f3f65fa432a1))
- Removed submodule documentation because it is already included in DataFoundry ([3010a39](https://gitlab.tue.nl/id-dept/id-projects/-/commit/3010a3988a6918c5d98f5f00b206e83821dc4d33))
- Removed old documentation ([56e60d8](https://gitlab.tue.nl/id-dept/id-projects/-/commit/56e60d879a5c76e3d3907682e98f41d91b3486bd))

### Fixed
- Fixed new project type assignment ([1dd2230](https://gitlab.tue.nl/id-dept/id-projects/-/commit/1dd223095172370bc450147bc60cd4840b61339e))

## [v0.9.12] - 2026-01-26
### Security
- Security update for Play 3.0.10, can be deployed without any other changes. ([d7824cf](https://gitlab.tue.nl/id-dept/id-projects/-/commit/d7824cf08ef34ecc56fea15e9a0cdfc78aee956a))
- Fix security module configuration in case there is no SSO configured ([fad834b](https://gitlab.tue.nl/id-dept/id-projects/-/commit/fad834b919a1aff591b7314f735a278ffe101d58))

### Added
- Added TU/e data-protection guidelines back into a new content hosting structure ([a420d8a](https://gitlab.tue.nl/id-dept/id-projects/-/commit/a420d8aafa23dbcd7c16aadba98d512a54650ca7))
- Added project links / references in project view ([48ac59d](https://gitlab.tue.nl/id-dept/id-projects/-/commit/48ac59daa5042e32900a60524f972ea04b3b4593))
- Added IDP about page ([1eb2fed](https://gitlab.tue.nl/id-dept/id-projects/-/commit/1eb2fed3c752890c8233e2083e39578d8ce0563d))
- Added project linking (outgoing project references) to idporject collections ([68c908d](https://gitlab.tue.nl/id-dept/id-projects/-/commit/68c908df10e3272e8a0d7142918f4894506d6d4c))
- Added icon in kiosk start button ([a3f53eb](https://gitlab.tue.nl/id-dept/id-projects/-/commit/a3f53ebbbe451b18a64b1c58072819e48c7398d3))
- Added showtime mode, auto-play, styling footer ([4e627f7](https://gitlab.tue.nl/id-dept/id-projects/-/commit/4e627f7932f09fa98dda3f1d80fdc4961e44fea1))
- Added kiosk footer ([e7ce77c](https://gitlab.tue.nl/id-dept/id-projects/-/commit/e7ce77c9709dad7e54a34ec271d0219226207de0))
- Added year filter ot kiosk setup ([e7ce77c](https://gitlab.tue.nl/id-dept/id-projects/-/commit/e7ce77c9709dad7e54a34ec271d0219226207de0))
- Added kiosk presentation mode to go through a selection of projects in a slideshow ([504d31c](https://gitlab.tue.nl/id-dept/id-projects/-/commit/504d31ce282b6caed5f48ce9c047ab156a7cda3f))
- Added submodule with symlink ([6ab8dfd](https://gitlab.tue.nl/id-dept/id-projects/-/commit/6ab8dfdde16815b49ed6b37bd152fa63ae1b722e))
- Added API access for labnotes ([f1968c1](https://gitlab.tue.nl/id-dept/id-projects/-/commit/f1968c1cb0d30577c24ce5bd19d0a15b2e57b052))
- Added JSON serialization for person and labnotes ([f1968c1](https://gitlab.tue.nl/id-dept/id-projects/-/commit/f1968c1cb0d30577c24ce5bd19d0a15b2e57b052))
- Added configuration option for max active projects ([f1968c1](https://gitlab.tue.nl/id-dept/id-projects/-/commit/f1968c1cb0d30577c24ce5bd19d0a15b2e57b052))
- Added configuration helper for int values ([f1968c1](https://gitlab.tue.nl/id-dept/id-projects/-/commit/f1968c1cb0d30577c24ce5bd19d0a15b2e57b052))
- Added orcid id setting for collaborators upon export ([adb238d](https://gitlab.tue.nl/id-dept/id-projects/-/commit/adb238dc973c57d26c1bedeb6243960b75d25a2d))
- Added shortening helper for filenames ([e719975](https://gitlab.tue.nl/id-dept/id-projects/-/commit/e7199752fefc608826d323b55328ab52b40b571e))
- Add uppercase file extensions to media DS view ([5547a3b](https://gitlab.tue.nl/id-dept/id-projects/-/commit/5547a3b2f9a0d4cac20ae5f4a7370e1a67a42625))
- Added project and dataset ref ids in title, for debugging and admin purposes ([118e9a9](https://gitlab.tue.nl/id-dept/id-projects/-/commit/118e9a9ca45ebf1c5fb81a9e39b653c7444d6bf0))

### Changed
- Updated knowledge base ([ade3c1e](https://gitlab.tue.nl/id-dept/id-projects/-/commit/ade3c1ea290e5765655095f3eee3afcf08727a7e))
- Updates for the local-ai library (which will be moved shortly anyway) ([7643c05](https://gitlab.tue.nl/id-dept/id-projects/-/commit/7643c05415f17a608248698b00a4d460a32a60d1))
- New attempt project linking, now in ProjectsController ([34b7d0c](https://gitlab.tue.nl/id-dept/id-projects/-/commit/34b7d0cd501b65db3d747609f27b6a59678f00ce))
- Changed name of fresh prince to a more mundane frank prince ([5f69130](https://gitlab.tue.nl/id-dept/id-projects/-/commit/5f691305a39fa65de97bc9cd46067b05eac270ca))
- Updates DF ([dd62878](https://gitlab.tue.nl/id-dept/id-projects/-/commit/dd62878dcb4c2ceab89b58f560d6faabff63554c))
- Df update ([6531818](https://gitlab.tue.nl/id-dept/id-projects/-/commit/6531818a9ccaccbe410c8f86796c58a5c44f563e))
- Switched styling to materialize ([fe2870f](https://gitlab.tue.nl/id-dept/id-projects/-/commit/fe2870fa6a23268f88e56191d54a8159c2fddc68))
- Reverted to original version of sbt-terser plugin ([b1b1be1](https://gitlab.tue.nl/id-dept/id-projects/-/commit/b1b1be14ea7a833057339860860239f5017d0f76))
- Switched to submodule for DataFoundry common code ([81d8158](https://gitlab.tue.nl/id-dept/id-projects/-/commit/81d815805ac049d3b8f417337813dbe8a53b8e32))
- Minor refactoring ([f1968c1](https://gitlab.tue.nl/id-dept/id-projects/-/commit/f1968c1cb0d30577c24ce5bd19d0a15b2e57b052))
- Extended swagger docs for project api methods ([f1968c1](https://gitlab.tue.nl/id-dept/id-projects/-/commit/f1968c1cb0d30577c24ce5bd19d0a15b2e57b052))
- Changed routes to start with project id, then list the api route ([f1968c1](https://gitlab.tue.nl/id-dept/id-projects/-/commit/f1968c1cb0d30577c24ce5bd19d0a15b2e57b052))
- Moved date and time-specific methods from dataset utils to date utils ([0e26d6b](https://gitlab.tue.nl/id-dept/id-projects/-/commit/0e26d6b3e2d87c764b056c47fc0696a5c6397634))
- Created utils.export package ([adb238d](https://gitlab.tue.nl/id-dept/id-projects/-/commit/adb238dc973c57d26c1bedeb6243960b75d25a2d))
- Moved assets helper into new package ([adb238d](https://gitlab.tue.nl/id-dept/id-projects/-/commit/adb238dc973c57d26c1bedeb6243960b75d25a2d))
- Moved VERSION string to string utils ([adb238d](https://gitlab.tue.nl/id-dept/id-projects/-/commit/adb238dc973c57d26c1bedeb6243960b75d25a2d))
- Created new metadata util class ([adb238d](https://gitlab.tue.nl/id-dept/id-projects/-/commit/adb238dc973c57d26c1bedeb6243960b75d25a2d))
- Consolidated json serialization of projects and datasets into metadata utils ([adb238d](https://gitlab.tue.nl/id-dept/id-projects/-/commit/adb238dc973c57d26c1bedeb6243960b75d25a2d))
- Changes according to changed store file in complete DS ([6aaa0fa](https://gitlab.tue.nl/id-dept/id-projects/-/commit/6aaa0faf292c49f4c9014c49ca3fd7fb075a1567))
- Rewrite store file for complete DS to not store multiple versions of a file, instead: just replace on re-upload ([1f10d11](https://gitlab.tue.nl/id-dept/id-projects/-/commit/1f10d11c0e4b95ef0c67d802963c07ae6d0c2831))
- Special case for media DS when participants upload files ([1f10d11](https://gitlab.tue.nl/id-dept/id-projects/-/commit/1f10d11c0e4b95ef0c67d802963c07ae6d0c2831))
- Refactor store file signature without timestamp ([1f10d11](https://gitlab.tue.nl/id-dept/id-projects/-/commit/1f10d11c0e4b95ef0c67d802963c07ae6d0c2831))
- Change how file lists in complete DS and subclasses are retrieved: just the latest version of a file ([1f10d11](https://gitlab.tue.nl/id-dept/id-projects/-/commit/1f10d11c0e4b95ef0c67d802963c07ae6d0c2831))
- Changed clients of complete DS to use the Optional<File> return more sensibly ([1f10d11](https://gitlab.tue.nl/id-dept/id-projects/-/commit/1f10d11c0e4b95ef0c67d802963c07ae6d0c2831))
- Minor logic fix ([243444b](https://gitlab.tue.nl/id-dept/id-projects/-/commit/243444bd7484f817ee197d83604aeebce26faed0))

### Removed
- Remove DF commons to be replaced by a submodule ([26ae001](https://gitlab.tue.nl/id-dept/id-projects/-/commit/26ae0016a12349819f26005067ab74c4ec02e004))
- Removed project-inherited meta data for datasets ([adb238d](https://gitlab.tue.nl/id-dept/id-projects/-/commit/adb238dc973c57d26c1bedeb6243960b75d25a2d))
- Removed deprecated methods in complete DS and media DS ([1f10d11](https://gitlab.tue.nl/id-dept/id-projects/-/commit/1f10d11c0e4b95ef0c67d802963c07ae6d0c2831))
- Removed db checks for files if possible (--> speed-up) ([1f10d11](https://gitlab.tue.nl/id-dept/id-projects/-/commit/1f10d11c0e4b95ef0c67d802963c07ae6d0c2831))

### Fixed
- Fixed reference display ([346edc7](https://gitlab.tue.nl/id-dept/id-projects/-/commit/346edc768072fd307cffa7ba72fc4dff0deb7a34))
- Fixed the project linking code ([bcbf94f](https://gitlab.tue.nl/id-dept/id-projects/-/commit/bcbf94f09341f255ed036ff0e12e310445adb53a))
- Fixed drag and drop logic ([681f65d](https://gitlab.tue.nl/id-dept/id-projects/-/commit/681f65d91630c2fbec5510154b144b83664373af))
- Fixed positioning of the kiosk --> showtime button in several feeding views ([a3f53eb](https://gitlab.tue.nl/id-dept/id-projects/-/commit/a3f53ebbbe451b18a64b1c58072819e48c7398d3))
- Fixed drag and drop ([41f5f30](https://gitlab.tue.nl/id-dept/id-projects/-/commit/41f5f30a954062af60a04dd3ac8b3e131eafa471))
- Fixed styling in kiosk ([49fee0d](https://gitlab.tue.nl/id-dept/id-projects/-/commit/49fee0dfb88e14fdf46d7b27ec47f909194f27e1))
- Fixed layout for kiosk config ([e7ce77c](https://gitlab.tue.nl/id-dept/id-projects/-/commit/e7ce77c9709dad7e54a34ec271d0219226207de0))
- Fixed OOCSI streaming for several DSs ([1f10d11](https://gitlab.tue.nl/id-dept/id-projects/-/commit/1f10d11c0e4b95ef0c67d802963c07ae6d0c2831))
- Fixed filename sanitization helper for umlauts etc. ([e719975](https://gitlab.tue.nl/id-dept/id-projects/-/commit/e7199752fefc608826d323b55328ab52b40b571e))
- Fixed aspect ratio of images in annotation dialog ([5547a3b](https://gitlab.tue.nl/id-dept/id-projects/-/commit/5547a3b2f9a0d4cac20ae5f4a7370e1a67a42625))

## [v0.9.10] - 2025-12-18
### Added
- Added user prompt pre-processing to incorporate context better ([f2d3dc5](https://gitlab.tue.nl/id-dept/id-projects/-/commit/f2d3dc5d56af1709d7caf82025661facf6f9c672))
- Added first full implementation of chatbot controller ([5ac0cd5](https://gitlab.tue.nl/id-dept/id-projects/-/commit/5ac0cd5e38515151b51223af72d8e8409a24e35e))
- Added constants for chatbot creation ([5ac0cd5](https://gitlab.tue.nl/id-dept/id-projects/-/commit/5ac0cd5e38515151b51223af72d8e8409a24e35e))
- Added intra-page link for API access ([5ac0cd5](https://gitlab.tue.nl/id-dept/id-projects/-/commit/5ac0cd5e38515151b51223af72d8e8409a24e35e))
- Added text embedding API task for local AI ([3db7afa](https://gitlab.tue.nl/id-dept/id-projects/-/commit/3db7afa155bd7ab10a5691a13e9a5c58dbd917b0))
- Added PDF file type for checking uploaded documents ([3db7afa](https://gitlab.tue.nl/id-dept/id-projects/-/commit/3db7afa155bd7ab10a5691a13e9a5c58dbd917b0))
- Added first and last name updating from SSO login ([faf29c1](https://gitlab.tue.nl/id-dept/id-projects/-/commit/faf29c1cdc4dfaed01feae990f828f9e2c4d778d))

### Changed
- Improved HTMX responses ([f2d3dc5](https://gitlab.tue.nl/id-dept/id-projects/-/commit/f2d3dc5d56af1709d7caf82025661facf6f9c672))
- Improved chat UI ([f2d3dc5](https://gitlab.tue.nl/id-dept/id-projects/-/commit/f2d3dc5d56af1709d7caf82025661facf6f9c672))
- Renamed variables in media to text controller ([1ff8880](https://gitlab.tue.nl/id-dept/id-projects/-/commit/1ff8880938a4a76546a19773f7c1eb2e69f712e6))

### Fixed
- Fixed the OIDC client for non-Azure SSO setups ([6070777](https://gitlab.tue.nl/id-dept/id-projects/-/commit/607077790c48a66b53a4ba76e89bd025f7e6cc33))
- Fixed default system prompt ([f2d3dc5](https://gitlab.tue.nl/id-dept/id-projects/-/commit/f2d3dc5d56af1709d7caf82025661facf6f9c672))
- Fixed issue with long file names ([f2d3dc5](https://gitlab.tue.nl/id-dept/id-projects/-/commit/f2d3dc5d56af1709d7caf82025661facf6f9c672))
- Fixed issue with too long chunks ([f2d3dc5](https://gitlab.tue.nl/id-dept/id-projects/-/commit/f2d3dc5d56af1709d7caf82025661facf6f9c672))
- Fixed issue with indexing old files ([f2d3dc5](https://gitlab.tue.nl/id-dept/id-projects/-/commit/f2d3dc5d56af1709d7caf82025661facf6f9c672))
- Fixed search results for chunks ([f2d3dc5](https://gitlab.tue.nl/id-dept/id-projects/-/commit/f2d3dc5d56af1709d7caf82025661facf6f9c672))
- Fixed OOCSI forwarding of form DS submissions ([b8fe3e2](https://gitlab.tue.nl/id-dept/id-projects/-/commit/b8fe3e270411dd7e70b9175eca46e28ba7c77e65))
- Fixed Entity dataset use tutorial ([b8fe3e2](https://gitlab.tue.nl/id-dept/id-projects/-/commit/b8fe3e270411dd7e70b9175eca46e28ba7c77e65))
- Fixed caching for qr code ([f566b88](https://gitlab.tue.nl/id-dept/id-projects/-/commit/f566b8849017168d7e29ba7b89e34caaf3484bb1))

## [v0.9.9] - 2025-12-11
### Security
- Added OIDC profile to the home controller and security module ([1774944](https://gitlab.tue.nl/id-dept/id-projects/-/commit/17749446d35a1893b0c17820b4805603a8409c37))

### Changed
- Extended the patched logging in editFile to support multiple arguments ([948bdf5](https://gitlab.tue.nl/id-dept/id-projects/-/commit/948bdf59da001bd4624d7185f2c4a2c94f637763))
- Enhance console styling and splitview layout

Add z-index to console and update splitview layout. ([32c5c4a](https://gitlab.tue.nl/id-dept/id-projects/-/commit/32c5c4a5ebe2b1d6cd90aaaa7e229987b9849dc9))
- Updated reference.conf ([9bb76d5](https://gitlab.tue.nl/id-dept/id-projects/-/commit/9bb76d5beba180a074ccd706b50ac885f60c2269))
- Prevented repeated local AI models updates when local AI is not configured ([2357ddb](https://gitlab.tue.nl/id-dept/id-projects/-/commit/2357ddb76603475811571c92fc5fa8a214eda695))

### Removed
- Removed logging of exception in local AI model update ([2357ddb](https://gitlab.tue.nl/id-dept/id-projects/-/commit/2357ddb76603475811571c92fc5fa8a214eda695))

### Fixed
- Fixed typo of missing " ([ebab350](https://gitlab.tue.nl/id-dept/id-projects/-/commit/ebab3509365297b27adde23503dfe524c89dd3c7))
- Fixed and then removed email service ([305457c](https://gitlab.tue.nl/id-dept/id-projects/-/commit/305457c8e49ae4f2321e99e9bb968455c8743056))

## [v0.9.8] - 2025-11-11
### Added
- Added star icon for badges in IDP ([83b5285](https://gitlab.tue.nl/id-dept/id-projects/-/commit/83b52853c517bc3c45386b9ad23372f5308c8bab))
- Added console to capture console.log and console.error output directly in the frontend ([2dda8a6](https://gitlab.tue.nl/id-dept/id-projects/-/commit/2dda8a659ee2029cc4388e932a450184ac5183e2))
- Added exception capture ([2dda8a6](https://gitlab.tue.nl/id-dept/id-projects/-/commit/2dda8a659ee2029cc4388e932a450184ac5183e2))
- Added OpenID SSO implementation (test pending) ([38c07d6](https://gitlab.tue.nl/id-dept/id-projects/-/commit/38c07d6efd44d7b9af35e0c054c09b063cbc2865))
- Added redirect back ([30160cb](https://gitlab.tue.nl/id-dept/id-projects/-/commit/30160cb264d8b457b70893cf4e8b73dcfd7c5d55))
- Added shortcut redirects to a user's latest project and the latest dataset in a project (helpful for tutorials and tours) ([8eca9ff](https://gitlab.tue.nl/id-dept/id-projects/-/commit/8eca9ff9066446040af8b09ec40ae0affc956e22))
- Added special redirects to a simple page for framed (iframe) requests, instead of loading a project or landing page into the frame ([654397d](https://gitlab.tue.nl/id-dept/id-projects/-/commit/654397d56a7375d11553149e4f81bd810de396aa))

### Changed
- Activated all collections items for public views ([83b5285](https://gitlab.tue.nl/id-dept/id-projects/-/commit/83b52853c517bc3c45386b9ad23372f5308c8bab))
- Approved collection items will be sorted first and get a star badge ([83b5285](https://gitlab.tue.nl/id-dept/id-projects/-/commit/83b52853c517bc3c45386b9ad23372f5308c8bab))
- Improved layout and layout stability when scrolling and resizing ([2dda8a6](https://gitlab.tue.nl/id-dept/id-projects/-/commit/2dda8a659ee2029cc4388e932a450184ac5183e2))
- Comments, removed whitespace ([01d20c7](https://gitlab.tue.nl/id-dept/id-projects/-/commit/01d20c7581a234c9db8692cf74516d14789dea56))
- Simplified and centralized the SSO checks ([38c07d6](https://gitlab.tue.nl/id-dept/id-projects/-/commit/38c07d6efd44d7b9af35e0c054c09b063cbc2865))
- Improved pluralization of user-facing strings ([18a1ca2](https://gitlab.tue.nl/id-dept/id-projects/-/commit/18a1ca24525418ced79bb68e23f5699e5b290f28))

### Removed
- Removed error messages for framed requests ([654397d](https://gitlab.tue.nl/id-dept/id-projects/-/commit/654397d56a7375d11553149e4f81bd810de396aa))

### Fixed
- Fixed wording in hosting tour ([4a4135e](https://gitlab.tue.nl/id-dept/id-projects/-/commit/4a4135e67c83302d953a1d43e22c720049b405ee))
- Fixed collapsed sidebar for responsive smaller layouts ([866821e](https://gitlab.tue.nl/id-dept/id-projects/-/commit/866821e4edc649787a9f655292e943e9c941773e))
- Fixed web action for file names with spaces (added url decode) ([654397d](https://gitlab.tue.nl/id-dept/id-projects/-/commit/654397d56a7375d11553149e4f81bd810de396aa))
- Fixed Fitbit authorization ([6ac2e5a](https://gitlab.tue.nl/id-dept/id-projects/-/commit/6ac2e5a03b524477ca6bf064dc9b2fb97b8e78f3))

## [v0.9.7] - 2025-11-07
### Added
- Added file editing with live preview, split view ([5f51790](https://gitlab.tue.nl/id-dept/id-projects/-/commit/5f51790981650ad4e80a4581f85fade0ca35099d))
- Added x-frames policy to support the loading of DF files in frames ([5f51790](https://gitlab.tue.nl/id-dept/id-projects/-/commit/5f51790981650ad4e80a4581f85fade0ca35099d))
- Added icons on sidebar ([0d90c74](https://gitlab.tue.nl/id-dept/id-projects/-/commit/0d90c74a0b6f9dd8ff5f2fd5b4a1cf02b3cc3b65))
- Added collapsed-layout ([0d90c74](https://gitlab.tue.nl/id-dept/id-projects/-/commit/0d90c74a0b6f9dd8ff5f2fd5b4a1cf02b3cc3b65))
- Added nss function with jsonnode parameter (same functionality) ([558eae8](https://gitlab.tue.nl/id-dept/id-projects/-/commit/558eae89a1a1ed659046d61a4ab5df53fa096dfa))
- Added front-end logging to IoT dataset API submissions ([b5c66f9](https://gitlab.tue.nl/id-dept/id-projects/-/commit/b5c66f90f110e64198a8a0698c700b2e72c8a2f0))

### Changed
- Convoscript update with cleaner syntax, context storage, better access functions for messages and context ([51ef150](https://gitlab.tue.nl/id-dept/id-projects/-/commit/51ef150274faa4dcdb23859e3c78c4137f576f1b))
- Adapted example to new convoscript version ([51ef150](https://gitlab.tue.nl/id-dept/id-projects/-/commit/51ef150274faa4dcdb23859e3c78c4137f576f1b))
- Improved CSS for convoscript ([51ef150](https://gitlab.tue.nl/id-dept/id-projects/-/commit/51ef150274faa4dcdb23859e3c78c4137f576f1b))
- Prevented redirects to components ([b5f70ca](https://gitlab.tue.nl/id-dept/id-projects/-/commit/b5f70ca3d792e16a324b9c3a8f3744924fec80ac))
- Extended the input for Timeseries record to nested json objects ([558eae8](https://gitlab.tue.nl/id-dept/id-projects/-/commit/558eae89a1a1ed659046d61a4ab5df53fa096dfa))
- Extended comment on which submission formats are supported ([b5c66f9](https://gitlab.tue.nl/id-dept/id-projects/-/commit/b5c66f90f110e64198a8a0698c700b2e72c8a2f0))
- Denied submissions without any data ([b5c66f9](https://gitlab.tue.nl/id-dept/id-projects/-/commit/b5c66f90f110e64198a8a0698c700b2e72c8a2f0))
- Only retrieve device from DB if the refId is provided in request ([b5c66f9](https://gitlab.tue.nl/id-dept/id-projects/-/commit/b5c66f90f110e64198a8a0698c700b2e72c8a2f0))
- Improved JSON handling in IoT dataset log method ([c8ec645](https://gitlab.tue.nl/id-dept/id-projects/-/commit/c8ec6453a48529c5d57af69fc4e7702f86ae6345))
- Further checks on json parsing across the code base ([c8ec645](https://gitlab.tue.nl/id-dept/id-projects/-/commit/c8ec6453a48529c5d57af69fc4e7702f86ae6345))
- Simplified the ApiRequest constructors ([c8ec645](https://gitlab.tue.nl/id-dept/id-projects/-/commit/c8ec6453a48529c5d57af69fc4e7702f86ae6345))
- Updated knowledge base ([f16c2bc](https://gitlab.tue.nl/id-dept/id-projects/-/commit/f16c2bc4ef4fd1a1a895420c947a8b0a62cb8720))

### Removed
- Removed unnecessary headers for unauthorized redirects (they are set in user auth) ([b5f70ca](https://gitlab.tue.nl/id-dept/id-projects/-/commit/b5f70ca3d792e16a324b9c3a8f3744924fec80ac))

### Fixed
- Fixed comparison in fitbit controller ([54726dd](https://gitlab.tue.nl/id-dept/id-projects/-/commit/54726dd2600ce75939e842588520ed835874e985))
- Fixed log messages for cluster and wearable ([54726dd](https://gitlab.tue.nl/id-dept/id-projects/-/commit/54726dd2600ce75939e842588520ed835874e985))
- Fixed attribution for convoscript ([72a1c69](https://gitlab.tue.nl/id-dept/id-projects/-/commit/72a1c690fb290834ff8f25e1b11442b3e3596549))
- Fixed Python API code ([b5c66f9](https://gitlab.tue.nl/id-dept/id-projects/-/commit/b5c66f90f110e64198a8a0698c700b2e72c8a2f0))

## [v0.9.6] - 2025-10-29
### Added
- Added word count to reference check ([3c34a8c](https://gitlab.tue.nl/id-dept/id-projects/-/commit/3c34a8c0ac40a91efac4afe60ccd51683159b721))
- Added information about processing steps ([3c34a8c](https://gitlab.tue.nl/id-dept/id-projects/-/commit/3c34a8c0ac40a91efac4afe60ccd51683159b721))
- Added admin view for configuration values ([a7d86a6](https://gitlab.tue.nl/id-dept/id-projects/-/commit/a7d86a65312551bbff0a6b2bc68abc35afd0b699))
- Added helper methods for working with constants and the configuration ([a7d86a6](https://gitlab.tue.nl/id-dept/id-projects/-/commit/a7d86a65312551bbff0a6b2bc68abc35afd0b699))
- Added null pointer exception handle as fetching Fitbit data ([aaed12a](https://gitlab.tue.nl/id-dept/id-projects/-/commit/aaed12a7088916147dc6254f99eab0a742269995))

### Changed
- Improved parsing of text for a better reference block detection and omitting appendices ([3c34a8c](https://gitlab.tue.nl/id-dept/id-projects/-/commit/3c34a8c0ac40a91efac4afe60ccd51683159b721))
- Improved layout and table format ([3c34a8c](https://gitlab.tue.nl/id-dept/id-projects/-/commit/3c34a8c0ac40a91efac4afe60ccd51683159b721))
- Reorganized configuration path constants ([a7d86a6](https://gitlab.tue.nl/id-dept/id-projects/-/commit/a7d86a65312551bbff0a6b2bc68abc35afd0b699))
- Commit 'aaed12a7088916147dc6254f99eab0a742269995': ([45de749](https://gitlab.tue.nl/id-dept/id-projects/-/commit/45de74953afea8c848d230331cba1a1f9726f553))
- Update develop configuration for Fitbit dataset ([aaed12a](https://gitlab.tue.nl/id-dept/id-projects/-/commit/aaed12a7088916147dc6254f99eab0a742269995))
- Code formating ([aaed12a](https://gitlab.tue.nl/id-dept/id-projects/-/commit/aaed12a7088916147dc6254f99eab0a742269995))

### Fixed
- Fixed misinformation of wearables - added null pointer exception handle as fetching Fitbit data - fixed issue of fetching wearable list - update develop configuration for Fitbit dataset - fixed issue of collecting scopes as creating Fitbit dataset - code formating ([45de749](https://gitlab.tue.nl/id-dept/id-projects/-/commit/45de74953afea8c848d230331cba1a1f9726f553))
- Fixed misinformation of wearables ([aaed12a](https://gitlab.tue.nl/id-dept/id-projects/-/commit/aaed12a7088916147dc6254f99eab0a742269995))
- Fixed issue of fetching wearable list ([aaed12a](https://gitlab.tue.nl/id-dept/id-projects/-/commit/aaed12a7088916147dc6254f99eab0a742269995))
- Fixed issue of collecting scopes as creating Fitbit dataset ([aaed12a](https://gitlab.tue.nl/id-dept/id-projects/-/commit/aaed12a7088916147dc6254f99eab0a742269995))

## [v0.9.5] - 2025-10-20
### Added
- Added access methods for stringified dataset creation date ([5aa9c59](https://gitlab.tue.nl/id-dept/id-projects/-/commit/5aa9c59f21001e2c029b11b1366c4112989bcc9d))
- Added dataset creation data to export ([5aa9c59](https://gitlab.tue.nl/id-dept/id-projects/-/commit/5aa9c59f21001e2c029b11b1366c4112989bcc9d))
- Added Apache Tika dependency to check file type with file contents ([836dffd](https://gitlab.tue.nl/id-dept/id-projects/-/commit/836dffd4603e0325f938750b4345f78bf870c92d))
- Added mime-type checks for file type ([836dffd](https://gitlab.tue.nl/id-dept/id-projects/-/commit/836dffd4603e0325f938750b4345f78bf870c92d))
- Added test dataset for file types ([836dffd](https://gitlab.tue.nl/id-dept/id-projects/-/commit/836dffd4603e0325f938750b4345f78bf870c92d))
- Added reference check tool ([17d2082](https://gitlab.tue.nl/id-dept/id-projects/-/commit/17d20827b2ec46b8fda000b4eb738c6c19f6dc62))
- Added PDF text extraction pipeline ([8df2079](https://gitlab.tue.nl/id-dept/id-projects/-/commit/8df207963190b9f32ed16bb5b71f56cc3faff002))
- Added blocking submit and process action ([8df2079](https://gitlab.tue.nl/id-dept/id-projects/-/commit/8df207963190b9f32ed16bb5b71f56cc3faff002))

### Changed
- Moved fileutil to filetypeutil, integrated with checks ([836dffd](https://gitlab.tue.nl/id-dept/id-projects/-/commit/836dffd4603e0325f938750b4345f78bf870c92d))
- Renamed checks to lookslike because the check is a weak one ([836dffd](https://gitlab.tue.nl/id-dept/id-projects/-/commit/836dffd4603e0325f938750b4345f78bf870c92d))
- Improved file type checks in all upload actions across controllers ([836dffd](https://gitlab.tue.nl/id-dept/id-projects/-/commit/836dffd4603e0325f938750b4345f78bf870c92d))
- Refactored tools to chat controller in id projects ([17d2082](https://gitlab.tue.nl/id-dept/id-projects/-/commit/17d20827b2ec46b8fda000b4eb738c6c19f6dc62))
- Refactored speech to text controller to media to text controller ([8df2079](https://gitlab.tue.nl/id-dept/id-projects/-/commit/8df207963190b9f32ed16bb5b71f56cc3faff002))

## [v0.9.4] - 2025-10-03
### Added
- Added publishing information in publish form ([0484106](https://gitlab.tue.nl/id-dept/id-projects/-/commit/0484106fdfe173183eea4dc6b863c6fc245092a7))
- Added publishing process stub and UI notifications ([0484106](https://gitlab.tue.nl/id-dept/id-projects/-/commit/0484106fdfe173183eea4dc6b863c6fc245092a7))
- Added user interface for existing qr code generator ([223f0e5](https://gitlab.tue.nl/id-dept/id-projects/-/commit/223f0e5c1b29ec4b55511170c9cdd3cf8fa49637))
- Added license setting to dataset controllers and views ([929844c](https://gitlab.tue.nl/id-dept/id-projects/-/commit/929844caf7320ba0118fbbdce12048e4831e3641))

### Fixed
- Fixed export format, part 1 (multiple licenses, readme fixes, dataset data files) ([a8d58b5](https://gitlab.tue.nl/id-dept/id-projects/-/commit/a8d58b5f8574338e2c5493c4fd7cd06a32b9d29a))

## [v0.9.3] - 2025-09-24
### Added
- Added local AI model page ([1dc0672](https://gitlab.tue.nl/id-dept/id-projects/-/commit/1dc06720bdd60e6e64d0850e4b15ee2ef4fb7b96))
- Added mandatory max_tokens for all requests ([81c361f](https://gitlab.tue.nl/id-dept/id-projects/-/commit/81c361f3177412b93699fbe57bd2946468b3789d))
- Added logging ([81c361f](https://gitlab.tue.nl/id-dept/id-projects/-/commit/81c361f3177412b93699fbe57bd2946468b3789d))

### Changed
- Cleaned commented code ([1d67750](https://gitlab.tue.nl/id-dept/id-projects/-/commit/1d6775017a9c93ea5fb0e5c37caeb873b5f24374))
- Hardcoded redirect URL, reverted to having a redirect url that is constructed from request ([1d67750](https://gitlab.tue.nl/id-dept/id-projects/-/commit/1d6775017a9c93ea5fb0e5c37caeb873b5f24374))
- Commit 'f6f4f56f82d28bdb73cdb9504bc4ca5b14fb5a97': ([e7ae27b](https://gitlab.tue.nl/id-dept/id-projects/-/commit/e7ae27b34a66491e0b97096b00c6680dfc7cca7a))
- Updated request format of GoogleFit API - fixed authentication of GoogleFit OAuth ([e7ae27b](https://gitlab.tue.nl/id-dept/id-projects/-/commit/e7ae27b34a66491e0b97096b00c6680dfc7cca7a))
- Commit '24ac7ff3f0a1124b9332ddd73bc95546ed0ccd98': ([b860c24](https://gitlab.tue.nl/id-dept/id-projects/-/commit/b860c2476adac3b4f796ccd071f60cba2448ef1e))
- Made all server properties optional in AI examples ([0eba7fb](https://gitlab.tue.nl/id-dept/id-projects/-/commit/0eba7fbf2bc4d24a1ad610a5abca98e2bf947e8a))
- Updated request format of GoogleFit API ([f6f4f56](https://gitlab.tue.nl/id-dept/id-projects/-/commit/f6f4f56f82d28bdb73cdb9504bc4ca5b14fb5a97))
- Main issue: the htmx is sending the delete request with spaces at the end of URL, which makes the id as a String paramter ([24ac7ff](https://gitlab.tue.nl/id-dept/id-projects/-/commit/24ac7ff3f0a1124b9332ddd73bc95546ed0ccd98))
- It takes around 10 seconds to update cache ([24ac7ff](https://gitlab.tue.nl/id-dept/id-projects/-/commit/24ac7ff3f0a1124b9332ddd73bc95546ed0ccd98))

### Removed
- Removed GoogleFit from dataset selection ([1d67750](https://gitlab.tue.nl/id-dept/id-projects/-/commit/1d6775017a9c93ea5fb0e5c37caeb873b5f24374))

### Fixed
- Fixed (temperally) the issue of deleting wearables - main issue: the htmx is sending the delete request with spaces at the end of URL, which makes the id as a String paramter - it takes around 10 seconds to update cache ([b860c24](https://gitlab.tue.nl/id-dept/id-projects/-/commit/b860c2476adac3b4f796ccd071f60cba2448ef1e))
- Fixed model mapping bugs ([81c361f](https://gitlab.tue.nl/id-dept/id-projects/-/commit/81c361f3177412b93699fbe57bd2946468b3789d))
- Fixed potential NPE in preprocessing request ([81c361f](https://gitlab.tue.nl/id-dept/id-projects/-/commit/81c361f3177412b93699fbe57bd2946468b3789d))
- Fixed authentication of GoogleFit OAuth ([f6f4f56](https://gitlab.tue.nl/id-dept/id-projects/-/commit/f6f4f56f82d28bdb73cdb9504bc4ca5b14fb5a97))
- Fixed (temperally) the issue of deleting wearables ([24ac7ff](https://gitlab.tue.nl/id-dept/id-projects/-/commit/24ac7ff3f0a1124b9332ddd73bc95546ed0ccd98))

## [v0.9.2] - 2025-09-24
### Added
- Ai tour to tourmanager
Added: AI. tour scala file
Added: new ID's to project edit page
Fixed: Improved text used in HostingTour ([42c6e02](https://gitlab.tue.nl/id-dept/id-projects/-/commit/42c6e02dfe5915d3271b94868bcdde2dd372492f))
- Added model metadata resolution from /models route ([4211c9f](https://gitlab.tue.nl/id-dept/id-projects/-/commit/4211c9f81c6adb76b4f3ba8aed1337fb0b27774b))
- Added model mapping for alias'ed models ([4211c9f](https://gitlab.tue.nl/id-dept/id-projects/-/commit/4211c9f81c6adb76b4f3ba8aed1337fb0b27774b))
- Added model resolution as a service, scheduled every 30 minutes ([4211c9f](https://gitlab.tue.nl/id-dept/id-projects/-/commit/4211c9f81c6adb76b4f3ba8aed1337fb0b27774b))
- Added "new file" to existing dataset ([57a8608](https://gitlab.tue.nl/id-dept/id-projects/-/commit/57a86084faafa09252ccb762cb92c025713bd5a5))

### Changed
- Moved localmodelmetadata from static access to injected singleton instance ([4211c9f](https://gitlab.tue.nl/id-dept/id-projects/-/commit/4211c9f81c6adb76b4f3ba8aed1337fb0b27774b))
- Refactored scheduledservice from abstract class to interface with default methods ([2ed0213](https://gitlab.tue.nl/id-dept/id-projects/-/commit/2ed02133dc291979a2ed03ae9a62a4a548a10c74))
- Updated knowledge base ([b6961a9](https://gitlab.tue.nl/id-dept/id-projects/-/commit/b6961a9a73fc59f51d58032ba9b7efaeb6a62fbc))
- Restrict project offers to public, tagged, reports ([570fce1](https://gitlab.tue.nl/id-dept/id-projects/-/commit/570fce18e5ebc0bd7553259a574ccb286350e73a))

### Fixed
- Fixed error logging ([52ec1f1](https://gitlab.tue.nl/id-dept/id-projects/-/commit/52ec1f1c8f00df1408f8d1e7349760ed7507cb5f))

## [v0.9.1] - 2025-08-31
### Added
- Added error message in case API access is not configured by admin ([315467d](https://gitlab.tue.nl/id-dept/id-projects/-/commit/315467d79380ab7cb16308b17023910a6fe255d2))
- Added the new academic year to knowledge base ([b3b0561](https://gitlab.tue.nl/id-dept/id-projects/-/commit/b3b056103b310a022bcd60f5720e6ae93ff94f1e))
- Added elements check for getScopeFromDataset() in Wearable ([62f0fbb](https://gitlab.tue.nl/id-dept/id-projects/-/commit/62f0fbbdeedb224e73dbd74bc2f943924d02468c))
- New tours and cleanup of tour files ([aee473a](https://gitlab.tue.nl/id-dept/id-projects/-/commit/aee473a77a8b3039396c03e403f22482c9edec8f))

### Changed
- Code cleanup ([b1014ba](https://gitlab.tue.nl/id-dept/id-projects/-/commit/b1014ba8ecbf83736ea8be508a2a41ce7f2637d3))
- Additional check for targetObject non-null ([b1014ba](https://gitlab.tue.nl/id-dept/id-projects/-/commit/b1014ba8ecbf83736ea8be508a2a41ce7f2637d3))
- Improved presentation of API key and related info in user view ([315467d](https://gitlab.tue.nl/id-dept/id-projects/-/commit/315467d79380ab7cb16308b17023910a6fe255d2))
- Improved distinction between script and chatbot ([8f4264c](https://gitlab.tue.nl/id-dept/id-projects/-/commit/8f4264ce90f736c23f13c193361780946789e5a2))
- Improved string HTML sanitization ([e815291](https://gitlab.tue.nl/id-dept/id-projects/-/commit/e8152916e4d0637d50ba5b1c249fad6a983931df))
- Updated scope list of Fitbit for new API ([9e5c923](https://gitlab.tue.nl/id-dept/id-projects/-/commit/9e5c9230acbdf46f4e0391fbd1ac79752e4319e9))
- Updated scope of Firbit for developing ([62f0fbb](https://gitlab.tue.nl/id-dept/id-projects/-/commit/62f0fbbdeedb224e73dbd74bc2f943924d02468c))
- Updated description for additional scope of authorization on signup wearable page ([66df02c](https://gitlab.tue.nl/id-dept/id-projects/-/commit/66df02cbff3503fb33583be8ad7fe87ea417fddb))
- Updated list of scopes on dataset page ([66df02c](https://gitlab.tue.nl/id-dept/id-projects/-/commit/66df02cbff3503fb33583be8ad7fe87ea417fddb))
- Switched support tiles for staff visibility ([06b2a1f](https://gitlab.tue.nl/id-dept/id-projects/-/commit/06b2a1f623512ed309dc977841654ca3e034e5bf))
- Moved df-activity styles into main SCSS ([340c1df](https://gitlab.tue.nl/id-dept/id-projects/-/commit/340c1df3fc43e96785fda9ff1c7af5904330ab1a))

### Removed
- Removed all mention of https://data.id.tue.nl from the documentation pages, replaced by current instance host ([d655fb0](https://gitlab.tue.nl/id-dept/id-projects/-/commit/d655fb01527e0dd5532a1ee50cc5ebf0bcaf14e9))

### Fixed
- Fixed language in fitbit sign-up ([b1014ba](https://gitlab.tue.nl/id-dept/id-projects/-/commit/b1014ba8ecbf83736ea8be508a2a41ce7f2637d3))
- Fixed dataset collection links for chatbot ([8f4264c](https://gitlab.tue.nl/id-dept/id-projects/-/commit/8f4264ce90f736c23f13c193361780946789e5a2))
- Fixed timeline display (filtering out non-alphanum chars which break the mermaid syntax) ([e815291](https://gitlab.tue.nl/id-dept/id-projects/-/commit/e8152916e4d0637d50ba5b1c249fad6a983931df))
- Fixed file upload dialog (separate field for individual files vs. files from an entire folder) ([66718ce](https://gitlab.tue.nl/id-dept/id-projects/-/commit/66718ce79a93a5e447801a7867a179bcf1a58235))
- Fixed issue of authorization procedure ([fd6175d](https://gitlab.tue.nl/id-dept/id-projects/-/commit/fd6175d29d28c19aac44c68c876aca40d8ad841d))
- Fixed error output for bulk team creation ([06b2a1f](https://gitlab.tue.nl/id-dept/id-projects/-/commit/06b2a1f623512ed309dc977841654ca3e034e5bf))

## [v0.9.0] - 2025-08-03
### Added
- Added limit argument/parameter to all data export calls ([fe466ff](https://gitlab.tue.nl/id-dept/id-projects/-/commit/fe466ff019538d24d7192240c66f4d73c5de3478))
- Added limit to respective database queries ([fe466ff](https://gitlab.tue.nl/id-dept/id-projects/-/commit/fe466ff019538d24d7192240c66f4d73c5de3478))
- Added controls to retrieve 100, 1000 or all rows in a table vis ([fe466ff](https://gitlab.tue.nl/id-dept/id-projects/-/commit/fe466ff019538d24d7192240c66f4d73c5de3478))
- Added hx-boost and transitions to main sidebar ([893ce86](https://gitlab.tue.nl/id-dept/id-projects/-/commit/893ce86d0f312e719948ac34751c8e5231e73b32))
- Added bulk team invite to support tiles, visible only to staff members ([bcc0192](https://gitlab.tue.nl/id-dept/id-projects/-/commit/bcc01927b8e495a1db77a721dc995b7d0d63bfad))
- Added bulk team invite routes and actions ([bcc0192](https://gitlab.tue.nl/id-dept/id-projects/-/commit/bcc01927b8e495a1db77a721dc995b7d0d63bfad))
- Added sidebar menu to admin views ([7142b93](https://gitlab.tue.nl/id-dept/id-projects/-/commit/7142b936c11f49351b0808bd2570ae7d2ed2e3c2))
- Added transitions to admin page links ([7142b93](https://gitlab.tue.nl/id-dept/id-projects/-/commit/7142b936c11f49351b0808bd2570ae7d2ed2e3c2))
- Added user administration page ([7142b93](https://gitlab.tue.nl/id-dept/id-projects/-/commit/7142b936c11f49351b0808bd2570ae7d2ed2e3c2))
- Added user role editing ([7142b93](https://gitlab.tue.nl/id-dept/id-projects/-/commit/7142b936c11f49351b0808bd2570ae7d2ed2e3c2))
- Added roles to person object, flexibly stored in the identity property ([ba5285f](https://gitlab.tue.nl/id-dept/id-projects/-/commit/ba5285f3686a41bcf3302ec0f4997ee8180aeafc))
- Added accessors for role ([ba5285f](https://gitlab.tue.nl/id-dept/id-projects/-/commit/ba5285f3686a41bcf3302ec0f4997ee8180aeafc))
- Added role class as a repository for role-related functions, e.g., configuration parsing etc. ([ba5285f](https://gitlab.tue.nl/id-dept/id-projects/-/commit/ba5285f3686a41bcf3302ec0f4997ee8180aeafc))
- Added sensor logger CSV and JSON import with column/property selection and timestamp truncated to seconds (limiting data load on db) ([e695342](https://gitlab.tue.nl/id-dept/id-projects/-/commit/e695342c1238d8d8847b3bbaa79ba5bec0b53765))
- Added import option to data logger index page ([e695342](https://gitlab.tue.nl/id-dept/id-projects/-/commit/e695342c1238d8d8847b3bbaa79ba5bec0b53765))
- Added pagination to ID Projects search ([713b158](https://gitlab.tue.nl/id-dept/id-projects/-/commit/713b1589c95edd43dbf6b259afbf8f2a06c01ed4))
- Added view transitions ([713b158](https://gitlab.tue.nl/id-dept/id-projects/-/commit/713b1589c95edd43dbf6b259afbf8f2a06c01ed4))
- Added dynamic project count update ([713b158](https://gitlab.tue.nl/id-dept/id-projects/-/commit/713b1589c95edd43dbf6b259afbf8f2a06c01ed4))
- Added data logger tour with default select box UI for data logger index page ([bda771c](https://gitlab.tue.nl/id-dept/id-projects/-/commit/bda771ca0a19a137afd883a5b3cec04aa11fa3b9))

### Changed
- Changed views and controllers accordingly ([fe466ff](https://gitlab.tue.nl/id-dept/id-projects/-/commit/fe466ff019538d24d7192240c66f4d73c5de3478))
- Moved tools to tools/index ([0064354](https://gitlab.tue.nl/id-dept/id-projects/-/commit/0064354cdad359a91dfcea92ed83cdc9c31dc003))
- Distributed all admin tabs to different dedicated pages ([7142b93](https://gitlab.tue.nl/id-dept/id-projects/-/commit/7142b936c11f49351b0808bd2570ae7d2ed2e3c2))
- Speed up the search results by first searching for id, then filtering, then retrieving the full projects and images ([713b158](https://gitlab.tue.nl/id-dept/id-projects/-/commit/713b1589c95edd43dbf6b259afbf8f2a06c01ed4))
- Reduced lifecycle logging ([ca04a22](https://gitlab.tue.nl/id-dept/id-projects/-/commit/ca04a22a0d7cc0f3167ac23b4ccce7d882cd7eb2))

### Fixed
- Fixed index page project summaries ([7142b93](https://gitlab.tue.nl/id-dept/id-projects/-/commit/7142b936c11f49351b0808bd2570ae7d2ed2e3c2))
- Fixed htmx scroll instructions ([713b158](https://gitlab.tue.nl/id-dept/id-projects/-/commit/713b1589c95edd43dbf6b259afbf8f2a06c01ed4))

## Historical Releases (Pre-v0.9.0)

### [v0.8.x] - 2023-05-15 to 2025-07-28 (60 Releases: v0.8.0 – v0.8.59)

- **Local AI & LLMs**: Integrated local generative AI services, OpenAI-compatible proxy, ConvoScript runtime, and ViperIDE integration.
- **Interactive Notebooks**: Added Starboard notebook support with Pyodide client-side Python execution.
- **Framework Upgrade**: Upgraded to Play Framework 3.0.x and Apache Pekko runtime.
- **Survey & Media Tools**: Introduced narrative surveys, speech-to-text audio processing, and Telegram bot data collection.
- **Platform Views**: Created student showcase views, portfolio generation engines, and demoday visitor interaction flows.

### [v0.7.x] - 2021-10-18 to 2023-05-09 (54 Releases: v0.7.0 – v0.7.53)

- **Scripting Engine**: Integrated GraalVM polyglot JavaScript runtime for dynamic dataset transformations and custom reactive actors.
- **HTMX Adoption**: Initiated progressive migration to HTMX for interactive UI flows without full page reloads.
- **Materialize CSS Modernisation**: Upgraded to Materialize CSS 1.1 and modernized core component libraries.
- **Database & Evolutions**: Enhanced Ebean ORM migrations, query optimizations, and automated schema evolution scripts.
- **Analytics & Projections**: Advanced timeseries downsampling, CSV projections, and OOCSI stream watchdog monitoring.

### [v0.6.x] - 2021-03-01 to 2021-10-18 (31 Releases: v0.6.0 – v0.6.30)

- **Production Graduation**: Official graduation out of beta with the `v0.6.0` release.
- **OpenAPI / Swagger**: Designed and deployed OpenAPI v2 documentation and standardized REST endpoints.
- **File-based Buffering**: Implemented disk-backed buffering for high-throughput dataset exports (preventing memory exhaustion on 10k+ record sets).
- **Study Management**: Multi-dataset participant sign-up, authorization token hashing, and GDPR-aligned data access rules.

### [v0.5.x] - 2020-01-26 to 2021-03-01 (48 Releases: v0.5.0 – v0.5.50)

- **Multi-Dataset Architecture**: Introduced specialized dataset categories: IoT (timeseries), Entity (key-value), Media, Diary, Survey, and Wearables (Fitbit & Google Fit).
- **OOCSI Real-time Ecosystem**: Integrated bi-directional OOCSI client connectivity for real-time messaging and device telemetry.
- **Device & Cluster Management**: Added physical cluster partitioning, device provisioning, and participant mapping.
- **Security & Administration**: Added automated database backup tools, server watchdog monitoring, and domain verification hosting.

### [v0.4.x] - 2019-11-03 to 2020-01-23 (4 Releases: v0.4.13 – v0.4.30)

- **Core Foundation**: Established initial Play Framework backend architecture, Ebean data models, and routing hierarchy.
- **Access Control**: Initial implementation of user authentication, project ownership, collaborator roles, and guest view permissions.
- **Data Ingestion**: Basic streaming CSV data export and timeseries record form ingestion stubs.

---

[v0.9.36]: https://github.com/id-data-foundry/data-foundry/compare/v0.9.35...development
[v0.9.35]: https://github.com/id-data-foundry/data-foundry/compare/v0.9.34...v0.9.35
[v0.9.34]: https://github.com/id-data-foundry/data-foundry/compare/v0.9.33...v0.9.34
[v0.9.33]: https://github.com/id-data-foundry/data-foundry/compare/v0.9.32...v0.9.33
[v0.9.32]: https://github.com/id-data-foundry/data-foundry/compare/v0.9.31...v0.9.32
[v0.9.31]: https://github.com/id-data-foundry/data-foundry/compare/v0.9.30...v0.9.31
[v0.9.30]: https://github.com/id-data-foundry/data-foundry/compare/v0.9.29...v0.9.30
[v0.9.29]: https://github.com/id-data-foundry/data-foundry/compare/v0.9.28...v0.9.29
[v0.9.28]: https://github.com/id-data-foundry/data-foundry/compare/v0.9.27...v0.9.28
[v0.9.27]: https://github.com/id-data-foundry/data-foundry/compare/v0.9.26...v0.9.27
[v0.9.26]: https://github.com/id-data-foundry/data-foundry/compare/v0.9.25...v0.9.26
[v0.9.25]: https://github.com/id-data-foundry/data-foundry/compare/v0.9.24...v0.9.25
[v0.9.24]: https://github.com/id-data-foundry/data-foundry/compare/v0.9.22...v0.9.24
[v0.9.23]: https://github.com/id-data-foundry/data-foundry/commit/ff7c031
[v0.9.22]: https://github.com/id-data-foundry/data-foundry/compare/v0.9.21...v0.9.22
[v0.9.21]: https://github.com/id-data-foundry/data-foundry/compare/v0.9.20...v0.9.21
[v0.9.20]: https://github.com/id-data-foundry/data-foundry/compare/v0.9.19...v0.9.20
[v0.9.19]: https://github.com/id-data-foundry/data-foundry/compare/v0.9.18...v0.9.19
[v0.9.18]: https://github.com/id-data-foundry/data-foundry/compare/v0.9.17...v0.9.18
[v0.9.17]: https://github.com/id-data-foundry/data-foundry/compare/v0.9.16...v0.9.17
[v0.9.16]: https://github.com/id-data-foundry/data-foundry/compare/v0.9.15...v0.9.16
[v0.9.15]: https://github.com/id-data-foundry/data-foundry/releases/tag/v0.9.15
[v0.9.14]: https://gitlab.tue.nl/id-dept/id-projects/-/compare/cba97f7b...c115e9a8
[v0.9.13]: https://gitlab.tue.nl/id-dept/id-projects/-/compare/8ba91ec0...cba97f7b
[v0.9.12]: https://gitlab.tue.nl/id-dept/id-projects/-/compare/faed795f...8ba91ec0
[v0.9.10]: https://gitlab.tue.nl/id-dept/id-projects/-/compare/427c70f9...faed795f
[v0.9.9]:  https://gitlab.tue.nl/id-dept/id-projects/-/compare/c1abe6be...427c70f9
[v0.9.8]:  https://gitlab.tue.nl/id-dept/id-projects/-/compare/c946fa8d...c1abe6be
[v0.9.7]:  https://gitlab.tue.nl/id-dept/id-projects/-/compare/46f1546a...c946fa8d
[v0.9.6]:  https://gitlab.tue.nl/id-dept/id-projects/-/compare/0e6699c8...46f1546a
[v0.9.5]:  https://gitlab.tue.nl/id-dept/id-projects/-/compare/017801b3...0e6699c8
[v0.9.4]:  https://gitlab.tue.nl/id-dept/id-projects/-/compare/6d427552...017801b3
[v0.9.3]:  https://gitlab.tue.nl/id-dept/id-projects/-/compare/d9561cac...6d427552
[v0.9.2]:  https://gitlab.tue.nl/id-dept/id-projects/-/compare/ef2d2f67...d9561cac
[v0.9.1]:  https://gitlab.tue.nl/id-dept/id-projects/-/compare/45f1cc4c...ef2d2f67
[v0.9.0]:  https://gitlab.tue.nl/id-dept/id-projects/-/compare/ac21ceb0...45f1cc4c
