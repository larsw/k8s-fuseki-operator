import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlExpression;
import org.apache.commons.jexl3.MapContext;

import com.google.gson.Gson;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

final class FusekiRangerAuthorizationFilter implements Filter {
	private static final Gson GSON = new Gson();
	private static final Duration DEFAULT_POLL_INTERVAL = Duration.ofSeconds(30);
	private static final Duration DEFAULT_HTTP_TIMEOUT = Duration.ofSeconds(10);
	private static final JexlEngine JEXL = new JexlBuilder().cache(512).strict(false).safe(true).silent(false).create();
	private static final Pattern CONDITION_MACRO_PATTERN = Pattern.compile("\\$\\{\\{\\s*([A-Za-z]+)\\.([^}]+?)\\s*}}", Pattern.CASE_INSENSITIVE);
	private static final List<DateTimeFormatter> ACCESS_TIME_FORMATTERS = List.of(
		DateTimeFormatter.ISO_INSTANT,
		DateTimeFormatter.ISO_OFFSET_DATE_TIME,
		DateTimeFormatter.ISO_ZONED_DATE_TIME,
		DateTimeFormatter.ISO_LOCAL_DATE_TIME,
		DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss"),
		DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
		DateTimeFormatter.ISO_LOCAL_DATE,
		DateTimeFormatter.ofPattern("yyyy/MM/dd")
	);

	private final boolean failClosed;
	private final RangerAccessCache accessCache;

	private FusekiRangerAuthorizationFilter(boolean failClosed, RangerAccessCache accessCache) {
		this.failClosed = failClosed;
		this.accessCache = accessCache;
	}

	static Filter fromEnvironment(boolean failClosed) throws IOException, ReflectiveOperationException {
		RangerAccessCache accessCache = RangerAccessCache.fromEnvironment(failClosed);
		accessCache.refreshPoliciesNow();
		return new FusekiRangerAuthorizationFilter(failClosed, accessCache);
	}

	@Override
	public void init(FilterConfig filterConfig) {}

	@Override
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
		if (!(request instanceof HttpServletRequest httpRequest) || !(response instanceof HttpServletResponse httpResponse)) {
			chain.doFilter(request, response);
			return;
		}

		FusekiAuthorizationFilter.AuthorizationDecision decision = authorize(httpRequest);
		if (decision.allowed()) {
			chain.doFilter(request, response);
			return;
		}

		httpResponse.sendError(decision.statusCode(), decision.message());
	}

	@Override
	public void destroy() {}

	private FusekiAuthorizationFilter.AuthorizationDecision authorize(HttpServletRequest request) {
		FusekiAuthorizationFilter.RequestTarget target = FusekiAuthorizationFilter.RequestTarget.from(request);
		if (target == null || target.isAdministrative()) {
			return FusekiAuthorizationFilter.AuthorizationDecision.allow();
		}

		FusekiAuthorizationFilter.RequestAction action = FusekiAuthorizationFilter.RequestAction.from(request, target);
		if (action == FusekiAuthorizationFilter.RequestAction.UNKNOWN) {
			return failClosed
				? FusekiAuthorizationFilter.AuthorizationDecision.deny(HttpServletResponse.SC_FORBIDDEN, "Forbidden")
				: FusekiAuthorizationFilter.AuthorizationDecision.allow();
		}

		RangerPolicySnapshot snapshot;
		FusekiAuthorizationFilter.RequestPrincipal principal = FusekiAuthorizationFilter.RequestPrincipal.from(request);
		try {
			snapshot = accessCache.currentPolicySnapshot();
			principal = accessCache.resolvePrincipal(principal);
		} catch (IOException exception) {
			return failClosed
				? FusekiAuthorizationFilter.AuthorizationDecision.deny(HttpServletResponse.SC_SERVICE_UNAVAILABLE, "Ranger authorization unavailable")
				: FusekiAuthorizationFilter.AuthorizationDecision.allow();
		}

		if (target.namedGraphs().isEmpty()) {
			return snapshot.authorize(principal, target, action, null, request);
		}

		for (String namedGraph : target.namedGraphs()) {
			FusekiAuthorizationFilter.AuthorizationDecision decision = snapshot.authorize(principal, target, action, namedGraph, request);
			if (!decision.allowed()) {
				return decision;
			}
		}
		return FusekiAuthorizationFilter.AuthorizationDecision.allow();
	}

	private record RangerConfiguration(String adminUrl, String serviceName, String username, String password, Duration pollInterval) {
		static RangerConfiguration fromEnvironment(boolean failClosed) throws IOException {
			Properties properties = loadProperties();
			String adminUrl = firstNonBlank(System.getenv("SECURITY_PROFILE_RANGER_ADMIN_URL"), properties.getProperty("ranger.adminURL"));
			String serviceName = firstNonBlank(System.getenv("SECURITY_PROFILE_RANGER_SERVICE_NAME"), properties.getProperty("ranger.serviceName"));
			String username = firstNonBlank(System.getenv("SECURITY_PROFILE_RANGER_USERNAME"), properties.getProperty("ranger.username"));
			String password = firstNonBlank(System.getenv("SECURITY_PROFILE_RANGER_PASSWORD"), properties.getProperty("ranger.password"));
			Duration pollInterval = parseDuration(
				firstNonBlank(System.getenv("SECURITY_PROFILE_RANGER_POLL_INTERVAL"), properties.getProperty("ranger.pollInterval")),
				DEFAULT_POLL_INTERVAL
			);

			if ((adminUrl == null || adminUrl.isBlank() || serviceName == null || serviceName.isBlank()) && failClosed) {
				throw new IllegalStateException("Ranger authorization mode requires ranger.adminURL and ranger.serviceName");
			}

			return new RangerConfiguration(
				normalizeAdminUrl(adminUrl),
				Objects.toString(serviceName, ""),
				Objects.toString(username, ""),
				Objects.toString(password, ""),
				pollInterval
			);
		}

		private static Properties loadProperties() throws IOException {
			Path configPath = Path.of(System.getenv().getOrDefault("SECURITY_PROFILE_RANGER_CONFIG", "/fuseki-extra/authorization/ranger.properties"));
			Properties properties = new Properties();
			if (!Files.exists(configPath)) {
				return properties;
			}
			try (Reader reader = Files.newBufferedReader(configPath, StandardCharsets.UTF_8)) {
				properties.load(reader);
			}
			return properties;
		}

		private static String normalizeAdminUrl(String adminUrl) {
			if (adminUrl == null) {
				return "";
			}
			return adminUrl.endsWith("/") ? adminUrl.substring(0, adminUrl.length() - 1) : adminUrl;
		}
	}

	private static final class RangerAccessCache {
		private final boolean failClosed;
		private final RangerConfiguration configuration;
		private final HttpClient httpClient;
		private final Map<String, CachedUserRoles> roleCache = new ConcurrentHashMap<>();
		private volatile RangerPolicySnapshot policySnapshot = RangerPolicySnapshot.empty();
		private volatile Instant nextPolicyRefreshAt = Instant.EPOCH;

		private RangerAccessCache(boolean failClosed, RangerConfiguration configuration, HttpClient httpClient) {
			this.failClosed = failClosed;
			this.configuration = configuration;
			this.httpClient = httpClient;
		}

		static RangerAccessCache fromEnvironment(boolean failClosed) throws IOException {
			RangerConfiguration configuration = RangerConfiguration.fromEnvironment(failClosed);
			HttpClient httpClient = HttpClient.newBuilder().connectTimeout(DEFAULT_HTTP_TIMEOUT).build();
			return new RangerAccessCache(failClosed, configuration, httpClient);
		}

		RangerPolicySnapshot currentPolicySnapshot() throws IOException {
			if (Instant.now().isBefore(nextPolicyRefreshAt)) {
				if (policySnapshot.refreshError() != null) {
					throw policySnapshot.refreshError();
				}
				return policySnapshot;
			}
			return refreshPoliciesNow();
		}

		synchronized RangerPolicySnapshot refreshPoliciesNow() throws IOException {
			try {
				RangerPolicySnapshot refreshed = RangerPolicySnapshot.fromPolicies(fetchPolicies(), null);
				policySnapshot = refreshed;
				nextPolicyRefreshAt = Instant.now().plus(configuration.pollInterval());
				return refreshed;
			} catch (IOException exception) {
				policySnapshot = policySnapshot.withRefreshError(exception);
				nextPolicyRefreshAt = Instant.now().plus(configuration.pollInterval());
				if (failClosed) {
					throw exception;
				}
				return policySnapshot;
			}
		}

		FusekiAuthorizationFilter.RequestPrincipal resolvePrincipal(FusekiAuthorizationFilter.RequestPrincipal principal) throws IOException {
			String user = principal.user();
			if (user == null || user.isBlank()) {
				return principal;
			}

			CachedUserRoles cached = roleCache.get(user);
			Instant now = Instant.now();
			if (cached != null && now.isBefore(cached.nextRefreshAt())) {
				if (cached.refreshError() != null) {
					throw cached.refreshError();
				}
				return principal.withAdditionalRoles(cached.roles());
			}

			return refreshUserRoles(principal);
		}

		private synchronized FusekiAuthorizationFilter.RequestPrincipal refreshUserRoles(FusekiAuthorizationFilter.RequestPrincipal principal) throws IOException {
			String user = principal.user();
			CachedUserRoles existing = roleCache.get(user);
			Instant now = Instant.now();
			if (existing != null && now.isBefore(existing.nextRefreshAt())) {
				if (existing.refreshError() != null) {
					throw existing.refreshError();
				}
				return principal.withAdditionalRoles(existing.roles());
			}

			try {
				Set<String> roles = Set.copyOf(fetchUserRoles(user));
				CachedUserRoles refreshed = new CachedUserRoles(roles, null, now.plus(configuration.pollInterval()));
				roleCache.put(user, refreshed);
				return principal.withAdditionalRoles(roles);
			} catch (IOException exception) {
				CachedUserRoles stale = new CachedUserRoles(
					existing == null ? Set.of() : existing.roles(),
					exception,
					now.plus(configuration.pollInterval())
				);
				roleCache.put(user, stale);
				if (failClosed) {
					throw exception;
				}
				return principal.withAdditionalRoles(stale.roles());
			}
		}

		private List<RangerPolicy> fetchPolicies() throws IOException {
			if (configuration.adminUrl().isBlank() || configuration.serviceName().isBlank()) {
				return List.of();
			}

			String policyUrl = configuration.adminUrl() + "/public/v2/api/service/" + urlEncode(configuration.serviceName()) + "/policy";
			HttpResponse<String> response = sendJsonGet(policyUrl);
			if (response.statusCode() < 200 || response.statusCode() >= 300) {
				throw new IOException("Ranger policy fetch failed with HTTP " + response.statusCode());
			}

			RangerPolicy[] policies = GSON.fromJson(response.body(), RangerPolicy[].class);
			return policies == null ? List.of() : List.of(policies);
		}

		private List<String> fetchUserRoles(String user) throws IOException {
			String rolesUrl = configuration.adminUrl() + "/public/v2/api/roles/user/" + urlEncode(user);
			HttpResponse<String> response = sendJsonGet(rolesUrl);
			if (response.statusCode() == HttpServletResponse.SC_NOT_FOUND) {
				return List.of();
			}
			if (response.statusCode() < 200 || response.statusCode() >= 300) {
				throw new IOException("Ranger role fetch failed with HTTP " + response.statusCode());
			}

			String[] roles = GSON.fromJson(response.body(), String[].class);
			return roles == null ? List.of() : List.of(roles);
		}

		private HttpResponse<String> sendJsonGet(String url) throws IOException {
			HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(URI.create(url))
				.header("Accept", "application/json")
				.timeout(DEFAULT_HTTP_TIMEOUT)
				.GET();
			if (!configuration.username().isBlank() || !configuration.password().isBlank()) {
				String token = Base64.getEncoder().encodeToString((configuration.username() + ":" + configuration.password()).getBytes(StandardCharsets.UTF_8));
				requestBuilder.header("Authorization", "Basic " + token);
			}

			try {
				return httpClient.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
			} catch (InterruptedException exception) {
				Thread.currentThread().interrupt();
				throw new IOException("Interrupted while calling Ranger admin", exception);
			}
		}
	}

	private record CachedUserRoles(Set<String> roles, IOException refreshError, Instant nextRefreshAt) {}

	private record RangerPolicySnapshot(List<RangerPolicy> policies, IOException refreshError) {
		static RangerPolicySnapshot empty() {
			return new RangerPolicySnapshot(List.of(), null);
		}

		static RangerPolicySnapshot fromPolicies(List<RangerPolicy> policies, IOException refreshError) {
			return new RangerPolicySnapshot(List.copyOf(policies), refreshError);
		}

		RangerPolicySnapshot withRefreshError(IOException refreshError) {
			return new RangerPolicySnapshot(policies, refreshError);
		}

		FusekiAuthorizationFilter.AuthorizationDecision authorize(
			FusekiAuthorizationFilter.RequestPrincipal principal,
			FusekiAuthorizationFilter.RequestTarget target,
			FusekiAuthorizationFilter.RequestAction action,
			String namedGraph,
			HttpServletRequest request
		) {
			boolean anyPolicyMatched = false;
			boolean allowed = false;
			RangerEvaluationContext context = RangerEvaluationContext.from(principal, target, action, namedGraph, request);

			for (RangerPolicy policy : policies) {
				if (!policy.isUsable() || !policy.matches(context)) {
					continue;
				}
				anyPolicyMatched = true;
				if (policy.denies(context)) {
					return FusekiAuthorizationFilter.AuthorizationDecision.deny(HttpServletResponse.SC_FORBIDDEN, "Forbidden");
				}
				if (policy.allows(context)) {
					allowed = true;
				}
			}

			if (allowed) {
				return FusekiAuthorizationFilter.AuthorizationDecision.allow();
			}
			if (!anyPolicyMatched) {
				return FusekiAuthorizationFilter.AuthorizationDecision.deny(HttpServletResponse.SC_FORBIDDEN, "Forbidden");
			}
			return FusekiAuthorizationFilter.AuthorizationDecision.deny(HttpServletResponse.SC_FORBIDDEN, "Forbidden");
		}
	}

	private static final class RangerPolicy {
		private String service;
		private Map<String, RangerPolicyResource> resources;
		private List<RangerPolicyCondition> conditions;
		private List<RangerPolicyItem> policyItems;
		private List<RangerPolicyItem> denyPolicyItems;
		private List<RangerPolicyItem> allowExceptions;
		private List<RangerPolicyItem> denyExceptions;
		private Boolean isEnabled;
		private Integer policyType;

		boolean isUsable() {
			return !Boolean.FALSE.equals(isEnabled) && (policyType == null || policyType.intValue() == 0);
		}

		boolean matches(RangerEvaluationContext context) {
			if (resources == null || resources.isEmpty()) {
				return false;
			}
			for (Map.Entry<String, RangerPolicyResource> entry : resources.entrySet()) {
				String requestValue = requestResourceValue(entry.getKey(), context);
				if (!entry.getValue().matches(requestValue)) {
					return false;
				}
			}
			return RangerPolicyCondition.matchesAll(conditions, context);
		}

		boolean denies(RangerEvaluationContext context) {
			boolean denied = matchesAny(denyPolicyItems, context);
			if (!denied) {
				return false;
			}
			return !matchesAny(denyExceptions, context);
		}

		boolean allows(RangerEvaluationContext context) {
			boolean allowed = matchesAny(policyItems, context);
			if (!allowed) {
				return false;
			}
			return !matchesAny(allowExceptions, context);
		}

		private static boolean matchesAny(List<RangerPolicyItem> items, RangerEvaluationContext context) {
			for (RangerPolicyItem item : emptyIfNull(items)) {
				if (item.matches(context)) {
					return true;
				}
			}
			return false;
		}

		private static String requestResourceValue(String resourceKey, RangerEvaluationContext context) {
			return switch (resourceKey.toLowerCase(Locale.ROOT)) {
				case "dataset", "repository", "database", "datasource" -> context.target().datasetName();
				case "graph", "namedgraph", "named_graph", "named-graph" -> context.namedGraph();
				case "endpoint", "operation" -> context.target().endpointName();
				case "path", "url", "uri" -> context.target().requestPath();
				default -> null;
			};
		}
	}

	private static final class RangerPolicyResource {
		private List<String> values;
		private Boolean isRecursive;
		private Boolean isExcludes;

		boolean matches(String requestValue) {
			boolean matched = false;
			for (String value : emptyIfNull(values)) {
				if (valueMatches(value, requestValue, Boolean.TRUE.equals(isRecursive))) {
					matched = true;
					break;
				}
			}
			return Boolean.TRUE.equals(isExcludes) ? !matched : matched;
		}
	}

	private static final class RangerPolicyItem {
		private List<String> users;
		private List<String> groups;
		private List<String> roles;
		private List<RangerPolicyItemAccess> accesses;
		private List<RangerPolicyCondition> conditions;

		boolean matches(RangerEvaluationContext context) {
			return matchesPrincipal(context.principal())
				&& matchesAccess(context.action())
				&& RangerPolicyCondition.matchesAll(conditions, context);
		}

		private boolean matchesPrincipal(FusekiAuthorizationFilter.RequestPrincipal principal) {
			if (emptyIfNull(users).isEmpty() && emptyIfNull(groups).isEmpty() && emptyIfNull(roles).isEmpty()) {
				return true;
			}

			String user = principal.user();
			if (!Objects.toString(user, "").isBlank()) {
				for (String candidate : emptyIfNull(users)) {
					if (matchesPrincipalValue(candidate, user)) {
						return true;
					}
				}
			}

			for (String group : principal.groups()) {
				for (String candidate : emptyIfNull(groups)) {
					if (matchesPrincipalValue(candidate, group)) {
						return true;
					}
				}
			}

			for (String role : principal.roles()) {
				for (String candidate : emptyIfNull(roles)) {
					if (matchesPrincipalValue(candidate, role)) {
						return true;
					}
				}
			}

			return false;
		}

		private boolean matchesAccess(FusekiAuthorizationFilter.RequestAction action) {
			for (RangerPolicyItemAccess access : emptyIfNull(accesses)) {
				if (access.matches(action)) {
					return true;
				}
			}
			return false;
		}
	}

	private static final class RangerPolicyItemAccess {
		private String type;
		private Boolean isAllowed;

		boolean matches(FusekiAuthorizationFilter.RequestAction action) {
			if (Boolean.FALSE.equals(isAllowed) || type == null || type.isBlank()) {
				return false;
			}
			return acceptedAccessTypes(action).contains(type.toLowerCase(Locale.ROOT));
		}
	}

	private static final class RangerPolicyCondition {
		private String type;
		private List<String> values;

		boolean matches(RangerEvaluationContext context) {
			if (emptyIfNull(values).isEmpty()) {
				return false;
			}

			String normalizedType = Objects.toString(type, "").toLowerCase(Locale.ROOT);
			if (isExpressionConditionType(normalizedType)) {
				return matchesExpressions(context);
			}
			if (normalizedType.contains("user")) {
				return matchesUser(context.principal().user());
			}
			if (normalizedType.contains("group")) {
				return matchesAnyPrincipalValue(values, context.principal().groups());
			}
			if (normalizedType.contains("role")) {
				return matchesAnyPrincipalValue(values, context.principal().roles());
			}
			if (normalizedType.contains("ip") || normalizedType.contains("address")) {
				return matchesAddress(context.remoteAddress());
			}
			if (normalizedType.contains("time") && values.size() >= 2) {
				return new AuthFunctions(context).isAccessTimeBetween(values.get(0), values.get(1));
			}
			return matchesExpressions(context);
		}

		private boolean matchesUser(String actualUser) {
			if (actualUser == null || actualUser.isBlank()) {
				return false;
			}
			for (String value : emptyIfNull(values)) {
				if (matchesPrincipalValue(value, actualUser)) {
					return true;
				}
			}
			return false;
		}

		private boolean matchesAddress(String remoteAddress) {
			for (String value : emptyIfNull(values)) {
				if (valueMatches(value, remoteAddress, false)) {
					return true;
				}
			}
			return false;
		}

		private boolean matchesExpressions(RangerEvaluationContext context) {
			for (String value : emptyIfNull(values)) {
				if (evaluateConditionExpression(value, context)) {
					return true;
				}
			}
			return false;
		}

		static boolean matchesAll(List<RangerPolicyCondition> conditions, RangerEvaluationContext context) {
			for (RangerPolicyCondition condition : emptyIfNull(conditions)) {
				if (!condition.matches(context)) {
					return false;
				}
			}
			return true;
		}
	}

	private static boolean isExpressionConditionType(String normalizedType) {
		return normalizedType.isBlank()
			|| normalizedType.contains("script")
			|| normalizedType.contains("expr")
			|| normalizedType.contains("evaluator");
	}

	private record RangerEvaluationContext(
		FusekiAuthorizationFilter.RequestPrincipal principal,
		FusekiAuthorizationFilter.RequestTarget target,
		FusekiAuthorizationFilter.RequestAction action,
		String namedGraph,
		String requestMethod,
		String remoteAddress,
		String queryString,
		Instant accessTime
	) {
		static RangerEvaluationContext from(
			FusekiAuthorizationFilter.RequestPrincipal principal,
			FusekiAuthorizationFilter.RequestTarget target,
			FusekiAuthorizationFilter.RequestAction action,
			String namedGraph,
			HttpServletRequest request
		) {
			return new RangerEvaluationContext(
				principal,
				target,
				action,
				namedGraph,
				request.getMethod(),
				Objects.toString(request.getRemoteAddr(), ""),
				Objects.toString(request.getQueryString(), ""),
				Instant.now()
			);
		}

		MapContext toJexlContext() {
			MapContext context = new MapContext();
			context.set("auth", new AuthFunctions(this));
			context.set("user", userAttributes());
			context.set("request", requestAttributes());
			context.set("resource", resourceAttributes());
			context.set("env", environmentAttributes());
			context.set("tag", Map.of());
			return context;
		}

		private Map<String, Object> userAttributes() {
			Map<String, Object> user = new LinkedHashMap<>();
			user.put("name", principal.user());
			user.put("username", principal.user());
			user.put("groups", List.copyOf(principal.groups()));
			user.put("roles", List.copyOf(principal.roles()));
			for (Map.Entry<String, Set<String>> entry : principal.claims().entrySet()) {
				user.put(entry.getKey(), scalarOrCollection(entry.getValue()));
			}
			return user;
		}

		private Map<String, Object> requestAttributes() {
			Map<String, Object> request = new LinkedHashMap<>();
			request.put("method", requestMethod);
			request.put("path", target.requestPath());
			request.put("remoteAddress", remoteAddress);
			request.put("queryString", queryString);
			return request;
		}

		private Map<String, Object> resourceAttributes() {
			Map<String, Object> resource = new LinkedHashMap<>();
			resource.put("dataset", target.datasetName());
			resource.put("endpoint", target.endpointName());
			resource.put("namedGraph", namedGraph);
			resource.put("graph", namedGraph);
			resource.put("path", target.requestPath());
			resource.put("action", action.name());
			return resource;
		}

		private Map<String, Object> environmentAttributes() {
			Map<String, Object> env = new LinkedHashMap<>();
			env.put("time", accessTime.toString());
			env.put("epochSeconds", accessTime.getEpochSecond());
			env.put("remoteAddress", remoteAddress);
			return env;
		}
	}

	private static final class AuthFunctions {
		private final RangerEvaluationContext context;

		private AuthFunctions(RangerEvaluationContext context) {
			this.context = context;
		}

		public boolean isInGroup(String group) {
			return group != null && context.principal().groups().contains(group);
		}

		public boolean isInRole(String role) {
			return role != null && context.principal().roles().contains(role);
		}

		public boolean isUser(String user) {
			return user != null && user.equals(context.principal().user());
		}

		public boolean hasTag(String tag) {
			return false;
		}

		public boolean isAccessTimeBetween(String startInclusive, String endInclusive) {
			Instant start = parseAccessTimeBoundary(startInclusive, false);
			Instant end = parseAccessTimeBoundary(endInclusive, true);
			if (start == null || end == null) {
				return false;
			}
			Instant current = context.accessTime();
			return !current.isBefore(start) && !current.isAfter(end);
		}
	}

	private static boolean evaluateConditionExpression(String rawExpression, RangerEvaluationContext context) {
		if (rawExpression == null || rawExpression.isBlank()) {
			return false;
		}
		try {
			JexlExpression expression = JEXL.createExpression(translateConditionExpression(rawExpression));
			Object value = expression.evaluate(context.toJexlContext());
			return asBoolean(value);
		} catch (RuntimeException exception) {
			return false;
		}
	}

	private static String translateConditionExpression(String expression) {
		String translated = rewriteConditionMacros(expression);
		translated = translated.replaceAll("\\bAND\\b", "&&");
		translated = translated.replaceAll("\\bOR\\b", "||");
		translated = translated.replaceAll("\\bNOT\\b", "!");
		translated = translated.replaceAll("\\bIS_IN_GROUP\\s*\\(", "auth.isInGroup(");
		translated = translated.replaceAll("\\bIS_IN_ROLE\\s*\\(", "auth.isInRole(");
		translated = translated.replaceAll("\\bIS_USER\\s*\\(", "auth.isUser(");
		translated = translated.replaceAll("\\bHAS_TAG\\s*\\(", "auth.hasTag(");
		translated = translated.replaceAll("\\bIS_ACCESS_TIME_BETWEEN\\s*\\(", "auth.isAccessTimeBetween(");
		translated = translated.replaceAll("\\bUSER\\.", "user.");
		translated = translated.replaceAll("\\bREQUEST\\.", "request.");
		translated = translated.replaceAll("\\bRESOURCE\\.", "resource.");
		translated = translated.replaceAll("\\bENV\\.", "env.");
		translated = translated.replaceAll("\\bTAG\\.", "tag.");
		return translated;
	}

	private static String rewriteConditionMacros(String expression) {
		Matcher matcher = CONDITION_MACRO_PATTERN.matcher(expression);
		StringBuffer output = new StringBuffer();
		while (matcher.find()) {
			String scope = matcher.group(1).toLowerCase(Locale.ROOT);
			String key = matcher.group(2).trim().replace("'", "\\'");
			matcher.appendReplacement(output, Matcher.quoteReplacement(scope + "['" + key + "']"));
		}
		matcher.appendTail(output);
		return output.toString();
	}

	private static Instant parseAccessTimeBoundary(String value, boolean inclusiveEnd) {
		if (value == null || value.isBlank()) {
			return null;
		}
		for (DateTimeFormatter formatter : ACCESS_TIME_FORMATTERS) {
			try {
				if (formatter == DateTimeFormatter.ISO_INSTANT) {
					return Instant.parse(value);
				}
				if (formatter == DateTimeFormatter.ISO_OFFSET_DATE_TIME) {
					return OffsetDateTime.parse(value, formatter).toInstant();
				}
				if (formatter == DateTimeFormatter.ISO_ZONED_DATE_TIME) {
					return ZonedDateTime.parse(value, formatter).toInstant();
				}
				if (formatter == DateTimeFormatter.ISO_LOCAL_DATE_TIME || formatter.equals(DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss")) || formatter.equals(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))) {
					return LocalDateTime.parse(value, formatter).toInstant(ZoneOffset.UTC);
				}
				LocalDate date = LocalDate.parse(value, formatter);
				return inclusiveEnd ? date.atTime(LocalTime.MAX).toInstant(ZoneOffset.UTC) : date.atStartOfDay().toInstant(ZoneOffset.UTC);
			} catch (DateTimeParseException ignored) {
			}
		}
		return null;
	}

	private static boolean matchesAnyPrincipalValue(List<String> expectedValues, Collection<String> actualValues) {
		for (String actual : actualValues) {
			for (String expected : emptyIfNull(expectedValues)) {
				if (matchesPrincipalValue(expected, actual)) {
					return true;
				}
			}
		}
		return false;
	}

	private static boolean asBoolean(Object value) {
		if (value instanceof Boolean bool) {
			return bool;
		}
		if (value instanceof Number number) {
			return number.doubleValue() != 0.0d;
		}
		if (value instanceof Collection<?> collection) {
			return !collection.isEmpty();
		}
		if (value instanceof String string) {
			return Boolean.parseBoolean(string) || !string.isBlank();
		}
		return value != null;
	}

	private static Object scalarOrCollection(Set<String> values) {
		if (values == null || values.isEmpty()) {
			return List.of();
		}
		return values.size() == 1 ? values.iterator().next() : List.copyOf(values);
	}

	private static String urlEncode(String value) {
		return URLEncoder.encode(value, StandardCharsets.UTF_8);
	}

	private static String firstNonBlank(String... values) {
		for (String value : values) {
			if (value != null && !value.isBlank()) {
				return value;
			}
		}
		return null;
	}

	private static Duration parseDuration(String value, Duration defaultValue) {
		if (value == null || value.isBlank()) {
			return defaultValue;
		}
		try {
			return Duration.parse(value);
		} catch (RuntimeException ignored) {
		}

		long totalNanos = 0L;
		int index = 0;
		while (index < value.length()) {
			int numberStart = index;
			while (index < value.length() && Character.isDigit(value.charAt(index))) {
				index++;
			}
			if (numberStart == index) {
				return defaultValue;
			}
			long amount = Long.parseLong(value.substring(numberStart, index));
			String unit;
			if (index + 1 < value.length() && (value.startsWith("ms", index) || value.startsWith("us", index) || value.startsWith("ns", index) || value.startsWith("µs", index))) {
				unit = value.substring(index, index + 2);
				index += 2;
			} else if (index < value.length()) {
				unit = value.substring(index, index + 1);
				index += 1;
			} else {
				return defaultValue;
			}

			totalNanos += switch (unit) {
				case "h" -> Duration.ofHours(amount).toNanos();
				case "m" -> Duration.ofMinutes(amount).toNanos();
				case "s" -> Duration.ofSeconds(amount).toNanos();
				case "ms" -> Duration.ofMillis(amount).toNanos();
				case "us", "µs" -> Duration.ofNanos(amount * 1_000L).toNanos();
				case "ns" -> amount;
				default -> Long.MIN_VALUE;
			};
			if (totalNanos < 0L) {
				return defaultValue;
			}
		}
		return totalNanos > 0L ? Duration.ofNanos(totalNanos) : defaultValue;
	}

	private static boolean matchesPrincipalValue(String candidate, String actual) {
		return "*".equals(candidate) || candidate.equals(actual);
	}

	private static Set<String> acceptedAccessTypes(FusekiAuthorizationFilter.RequestAction action) {
		return switch (action) {
			case QUERY -> Set.of("query", "read", "select");
			case READ -> Set.of("read", "query", "get");
			case UPDATE -> Set.of("update", "write", "modify");
			case WRITE -> Set.of("write", "update", "create", "delete");
			case UNKNOWN -> Set.of();
		};
	}

	private static boolean valueMatches(String pattern, String requestValue, boolean recursive) {
		if (pattern == null || pattern.isBlank()) {
			return false;
		}
		if (requestValue == null || requestValue.isBlank()) {
			return false;
		}
		if ("*".equals(pattern)) {
			return true;
		}
		if (pattern.equals(requestValue)) {
			return true;
		}
		if (recursive && requestValue.startsWith(pattern)) {
			return true;
		}
		if (!pattern.contains("*") && !pattern.contains("?")) {
			return false;
		}
		String regex = Pattern.quote(pattern).replace("\\*", ".*").replace("\\?", ".");
		return requestValue.matches(regex);
	}

	private static <T> List<T> emptyIfNull(List<T> values) {
		return values == null ? Collections.emptyList() : values;
	}
}