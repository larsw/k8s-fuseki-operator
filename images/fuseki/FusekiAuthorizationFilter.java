import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import org.apache.accumulo.access.AccessEvaluator;
import org.apache.accumulo.access.AccessExpression;
import org.apache.accumulo.access.Authorizations;

import com.google.gson.Gson;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

final class FusekiAuthorizationFilter implements Filter {
	private static final Gson GSON = new Gson();
	private static final String MODE_LOCAL = "local";
	private static final String MODE_RANGER = "ranger";
	private static final List<String> USER_HEADER_NAMES = List.of("X-Forwarded-User", "X-Remote-User");
	private static final List<String> GROUP_HEADER_NAMES = List.of("X-Forwarded-Groups", "X-Remote-Groups", "X-OIDC-Groups");
	private static final List<String> ROLE_HEADER_NAMES = List.of("X-Forwarded-Roles", "X-Remote-Roles", "X-Ranger-Roles");
	private static final List<String> AUTHORIZATION_LABEL_HEADER_NAMES = List.of("X-Authorization-Labels", "X-Authz-Labels");
	private static final List<String> NAMED_GRAPH_PARAMETER_NAMES = List.of(
		"graph",
		"graph-uri",
		"default-graph-uri",
		"named-graph-uri",
		"using-graph-uri",
		"using-named-graph-uri"
	);

	private final boolean failClosed;
	private final Map<String, DatasetPolicySet> datasetPolicies;

	private FusekiAuthorizationFilter(boolean failClosed, Map<String, DatasetPolicySet> datasetPolicies) {
		this.failClosed = failClosed;
		this.datasetPolicies = datasetPolicies;
	}

	static Filter createFromEnvironment() throws IOException, ReflectiveOperationException {
		boolean failClosed = Boolean.parseBoolean(readEnv("FUSEKI_AUTHORIZATION_FAIL_CLOSED", "true"));
		String mode = readEnv("SECURITY_PROFILE_AUTHORIZATION_MODE", "Local").trim().toLowerCase(Locale.ROOT);
		return switch (mode) {
			case MODE_LOCAL -> new FusekiAuthorizationFilter(failClosed, loadLocalPolicies(failClosed));
			case MODE_RANGER -> FusekiRangerAuthorizationFilter.fromEnvironment(failClosed);
			case "" -> new FusekiAuthorizationFilter(failClosed, loadLocalPolicies(failClosed));
			default -> {
				if (failClosed) {
					throw new IllegalStateException("Unsupported authorization mode: " + mode);
				}
				yield null;
			}
		};
	}

	@Override
	public void init(FilterConfig filterConfig) {}

	@Override
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
		if (!(request instanceof HttpServletRequest httpRequest) || !(response instanceof HttpServletResponse httpResponse)) {
			chain.doFilter(request, response);
			return;
		}

		AuthorizationDecision decision = authorize(httpRequest);
		if (decision.allowed()) {
			chain.doFilter(request, response);
			return;
		}

		httpResponse.sendError(decision.statusCode(), decision.message());
	}

	@Override
	public void destroy() {}

	private AuthorizationDecision authorize(HttpServletRequest request) {
		RequestTarget target = RequestTarget.from(request);
		if (target == null || target.isAdministrative()) {
			return AuthorizationDecision.allow();
		}

		DatasetPolicySet policySet = datasetPolicies.get(target.datasetName());
		if (policySet == null) {
			return AuthorizationDecision.allow();
		}

		RequestPrincipal principal = RequestPrincipal.from(request);
		if (principal.isAnonymous()) {
			return AuthorizationDecision.deny(HttpServletResponse.SC_FORBIDDEN, "Forbidden");
		}

		RequestAction action = RequestAction.from(request, target);
		if (action == RequestAction.UNKNOWN) {
			return failClosed
				? AuthorizationDecision.deny(HttpServletResponse.SC_FORBIDDEN, "Forbidden")
				: AuthorizationDecision.allow();
		}

		Set<String> namedGraphs = target.namedGraphs();
		if (namedGraphs.isEmpty()) {
			return policySet.authorize(principal, action, null);
		}

		for (String namedGraph : namedGraphs) {
			AuthorizationDecision graphDecision = policySet.authorize(principal, action, namedGraph);
			if (!graphDecision.allowed()) {
				return graphDecision;
			}
		}
		return AuthorizationDecision.allow();
	}

	private static Map<String, DatasetPolicySet> loadLocalPolicies(boolean failClosed) throws IOException {
		Path indexPath = Path.of(readEnv("FUSEKI_AUTHORIZATION_INDEX", "/fuseki-extra/authorization/policies.index"));
		if (!Files.exists(indexPath)) {
			if (failClosed) {
				throw new IllegalStateException("Local authorization mode requires an index file at " + indexPath);
			}
			return Map.of();
		}

		Map<String, DatasetPolicySet> policies = new LinkedHashMap<>();
		for (String line : Files.readAllLines(indexPath, StandardCharsets.UTF_8)) {
			String trimmed = line.trim();
			if (trimmed.isEmpty() || trimmed.startsWith("#")) {
				continue;
			}
			int separator = trimmed.indexOf('=');
			if (separator <= 0 || separator == trimmed.length() - 1) {
				if (failClosed) {
					throw new IllegalStateException("Invalid authorization index entry: " + trimmed);
				}
				continue;
			}

			String datasetName = trimmed.substring(0, separator).trim();
			Path bundlePath = Path.of(trimmed.substring(separator + 1).trim());
			if (!Files.exists(bundlePath)) {
				if (failClosed) {
					throw new IllegalStateException("Authorization bundle does not exist: " + bundlePath);
				}
				continue;
			}

			DatasetPolicySet policySet = DatasetPolicySet.fromBundle(datasetName, bundlePath, failClosed);
			if (!policySet.rules().isEmpty()) {
				policies.put(datasetName, policySet);
			}
		}
		return Map.copyOf(policies);
	}

	private static String readEnv(String name, String defaultValue) {
		String value = System.getenv(name);
		return value == null || value.isBlank() ? defaultValue : value;
	}

	static enum RequestAction {
		QUERY,
		READ,
		UPDATE,
		WRITE,
		UNKNOWN;

		static RequestAction from(HttpServletRequest request, RequestTarget target) {
			String endpoint = target.endpointName();
			String method = request.getMethod().toUpperCase(Locale.ROOT);
			if (hasParameter(request, "update") || endpoint.equals("update")) {
				return UPDATE;
			}
			if (endpoint.equals("data") || endpoint.equals("get") || endpoint.equals("upload")) {
				return switch (method) {
					case "GET", "HEAD" -> READ;
					case "POST", "PUT", "PATCH", "DELETE" -> WRITE;
					default -> UNKNOWN;
				};
			}
			if (hasParameter(request, "query") || endpoint.equals("query") || endpoint.equals("sparql") || endpoint.isEmpty()) {
				return QUERY;
			}
			return switch (method) {
				case "GET", "HEAD" -> READ;
				case "POST", "PUT", "PATCH", "DELETE" -> WRITE;
				default -> UNKNOWN;
			};
		}

		private static boolean hasParameter(HttpServletRequest request, String name) {
			String[] values = request.getParameterValues(name);
			return values != null && values.length > 0;
		}
	}

	static record AuthorizationDecision(boolean allowed, int statusCode, String message) {
		static AuthorizationDecision allow() {
			return new AuthorizationDecision(true, HttpServletResponse.SC_OK, "OK");
		}

		static AuthorizationDecision deny(int statusCode, String message) {
			return new AuthorizationDecision(false, statusCode, message);
		}
	}

	static record RequestTarget(String datasetName, String endpointName, String requestPath, Set<String> namedGraphs, boolean administrative) {
		static RequestTarget from(HttpServletRequest request) {
			String requestUri = request.getRequestURI();
			String contextPath = Objects.toString(request.getContextPath(), "");
			String effectivePath = requestUri.startsWith(contextPath) ? requestUri.substring(contextPath.length()) : requestUri;
			List<String> segments = new ArrayList<>();
			for (String segment : effectivePath.split("/")) {
				if (!segment.isBlank()) {
					segments.add(segment);
				}
			}
			if (segments.isEmpty()) {
				return null;
			}
			if (segments.get(0).equals("$")) {
				return new RequestTarget("", "", effectivePath, Set.of(), true);
			}
			String datasetName = segments.get(0);
			String endpointName = segments.size() > 1 ? segments.get(1) : "";
			Set<String> namedGraphs = namedGraphs(request);
			return new RequestTarget(datasetName, endpointName, effectivePath, namedGraphs, false);
		}

		private static Set<String> namedGraphs(HttpServletRequest request) {
			Set<String> graphs = new LinkedHashSet<>();
			for (String parameterName : NAMED_GRAPH_PARAMETER_NAMES) {
				String[] values = request.getParameterValues(parameterName);
				if (values == null) {
					continue;
				}
				for (String value : values) {
					if (value != null && !value.isBlank()) {
						graphs.add(value.trim());
					}
				}
			}
			return Set.copyOf(graphs);
		}

		boolean isAdministrative() {
			return administrative;
		}
	}

	static record RequestPrincipal(String user, Set<String> groups, Set<String> roles, Map<String, Set<String>> claims, Set<String> authorizations) {
		static RequestPrincipal from(HttpServletRequest request) {
			String user = firstNonBlank(request.getRemoteUser(), headers(request, USER_HEADER_NAMES));
			Set<String> groups = splitHeaders(request, GROUP_HEADER_NAMES);
			Set<String> roles = splitHeaders(request, ROLE_HEADER_NAMES);
			Map<String, Set<String>> claims = claims(request, groups, roles);
			Set<String> authorizations = new LinkedHashSet<>();
			if (user != null && !user.isBlank()) {
				authorizations.add(user);
			}
			authorizations.addAll(groups);
			authorizations.addAll(roles);
			for (Set<String> values : claims.values()) {
				authorizations.addAll(values);
			}
			authorizations.addAll(splitHeaders(request, AUTHORIZATION_LABEL_HEADER_NAMES));
			return new RequestPrincipal(user, Set.copyOf(groups), Set.copyOf(roles), Map.copyOf(claims), Set.copyOf(authorizations));
		}

		boolean isAnonymous() {
			return (user == null || user.isBlank()) && groups.isEmpty() && roles.isEmpty() && claims.isEmpty() && authorizations.isEmpty();
		}

		RequestPrincipal withAdditionalRoles(Set<String> additionalRoles) {
			if (additionalRoles == null || additionalRoles.isEmpty()) {
				return this;
			}

			Set<String> mergedRoles = new LinkedHashSet<>(roles);
			mergedRoles.addAll(additionalRoles);

			Map<String, Set<String>> mergedClaims = new HashMap<>(claims);
			mergedClaims.put("roles", Set.copyOf(mergedRoles));

			Set<String> mergedAuthorizations = new LinkedHashSet<>(authorizations);
			mergedAuthorizations.addAll(additionalRoles);

			return new RequestPrincipal(
				user,
				groups,
				Set.copyOf(mergedRoles),
				Map.copyOf(mergedClaims),
				Set.copyOf(mergedAuthorizations)
			);
		}

		boolean matchesSubject(Subject subject) {
			return switch (subject.type()) {
				case "User" -> user != null && user.equals(subject.value());
				case "Group" -> groups.contains(subject.value());
				case "OIDCClaim" -> claims.getOrDefault(subject.claim().toLowerCase(Locale.ROOT), Set.of()).contains(subject.value());
				default -> false;
			};
		}

		private static List<String> headers(HttpServletRequest request, List<String> names) {
			List<String> values = new ArrayList<>();
			for (String name : names) {
				String value = request.getHeader(name);
				if (value != null && !value.isBlank()) {
					values.add(value);
				}
			}
			return values;
		}

		private static Map<String, Set<String>> claims(HttpServletRequest request, Set<String> groups, Set<String> roles) {
			Map<String, Set<String>> claims = new HashMap<>();
			if (!groups.isEmpty()) {
				claims.put("groups", Set.copyOf(groups));
			}
			if (!roles.isEmpty()) {
				claims.put("roles", Set.copyOf(roles));
			}
			Enumeration<String> headerNames = request.getHeaderNames();
			if (headerNames == null) {
				return claims;
			}
			while (headerNames.hasMoreElements()) {
				String headerName = headerNames.nextElement();
				String normalizedHeaderName = headerName.toLowerCase(Locale.ROOT);
				if (!normalizedHeaderName.startsWith("x-oidc-claim-")) {
					continue;
				}
				String claimName = normalizedHeaderName.substring("x-oidc-claim-".length());
				Set<String> values = claims.computeIfAbsent(claimName, ignored -> new LinkedHashSet<>());
				Enumeration<String> headerValues = request.getHeaders(headerName);
				while (headerValues.hasMoreElements()) {
					values.addAll(splitDelimited(headerValues.nextElement()));
				}
			}
			Map<String, Set<String>> immutableClaims = new HashMap<>();
			for (Map.Entry<String, Set<String>> entry : claims.entrySet()) {
				immutableClaims.put(entry.getKey(), Set.copyOf(entry.getValue()));
			}
			return immutableClaims;
		}
	}

	private record DatasetPolicySet(String datasetName, List<PolicyRule> rules) {
		static DatasetPolicySet fromBundle(String datasetName, Path bundlePath, boolean failClosed) throws IOException {
			Bundle bundle;
			try (Reader reader = Files.newBufferedReader(bundlePath, StandardCharsets.UTF_8)) {
				bundle = GSON.fromJson(reader, Bundle.class);
			}
			if (bundle == null || bundle.policies == null) {
				if (failClosed) {
					throw new IllegalStateException("Authorization bundle is empty: " + bundlePath);
				}
				return new DatasetPolicySet(datasetName, List.of());
			}

			List<PolicyRule> rules = new ArrayList<>();
			for (Policy policy : bundle.policies) {
				if (policy == null || policy.rules == null) {
					continue;
				}
				for (Rule rule : policy.rules) {
					if (rule == null || rule.target == null) {
						continue;
					}
					if (!datasetName.equals(rule.target.datasetRef)) {
						continue;
					}
					try {
						AccessExpression.validate(rule.expression);
					} catch (RuntimeException exception) {
						if (failClosed) {
							throw new IllegalStateException(
								"Invalid " + rule.expressionType + " expression in " + bundlePath + ": " + rule.expression,
								exception
							);
						}
						continue;
					}
					rules.add(PolicyRule.fromRule(rule));
				}
			}
			return new DatasetPolicySet(datasetName, List.copyOf(rules));
		}

		AuthorizationDecision authorize(RequestPrincipal principal, RequestAction action, String namedGraph) {
			boolean allowed = false;
			for (PolicyRule rule : rules) {
				if (!rule.appliesTo(action, namedGraph)) {
					continue;
				}
				if (!rule.matches(principal)) {
					continue;
				}
				if (rule.deny()) {
					return AuthorizationDecision.deny(HttpServletResponse.SC_FORBIDDEN, "Forbidden");
				}
				allowed = true;
			}
			return allowed ? AuthorizationDecision.allow() : AuthorizationDecision.deny(HttpServletResponse.SC_FORBIDDEN, "Forbidden");
		}
	}

	private record PolicyRule(Set<RequestAction> actions, boolean deny, String namedGraph, String expression, List<Subject> subjects) {
		static PolicyRule fromRule(Rule rule) {
			Set<RequestAction> actions = new LinkedHashSet<>();
			for (String action : emptyIfNull(rule.actions)) {
				switch (action) {
					case "Query" -> actions.add(RequestAction.QUERY);
					case "Read" -> actions.add(RequestAction.READ);
					case "Update" -> actions.add(RequestAction.UPDATE);
					case "Write" -> actions.add(RequestAction.WRITE);
					case "Admin" -> {
					}
					default -> {
					}
				}
			}
			List<Subject> subjects = new ArrayList<>();
			for (RuleSubject subject : emptyIfNull(rule.subjects)) {
				subjects.add(new Subject(subject.type, subject.value, subject.claim == null ? "" : subject.claim));
			}
			return new PolicyRule(
				Set.copyOf(actions),
				"Deny".equals(rule.effect),
				rule.target.namedGraph == null || rule.target.namedGraph.isBlank() ? null : rule.target.namedGraph,
				rule.expression,
				List.copyOf(subjects)
			);
		}

		boolean appliesTo(RequestAction action, String requestNamedGraph) {
			if (!actions.contains(action)) {
				return false;
			}
			if (namedGraph == null) {
				return true;
			}
			return namedGraph.equals(requestNamedGraph);
		}

		boolean matches(RequestPrincipal principal) {
			boolean subjectMatched = false;
			for (Subject subject : subjects) {
				if (principal.matchesSubject(subject)) {
					subjectMatched = true;
					break;
				}
			}
			if (!subjectMatched) {
				return false;
			}
			AccessEvaluator evaluator = AccessEvaluator.of(Authorizations.of(principal.authorizations()));
			return evaluator.canAccess(expression);
		}
	}

	private record Subject(String type, String value, String claim) {}

	private static String firstNonBlank(String primary, List<String> fallbacks) {
		if (primary != null && !primary.isBlank()) {
			return primary;
		}
		for (String fallback : fallbacks) {
			if (fallback != null && !fallback.isBlank()) {
				return fallback;
			}
		}
		return null;
	}

	private static Set<String> splitHeaders(HttpServletRequest request, List<String> headerNames) {
		Set<String> values = new LinkedHashSet<>();
		for (String headerName : headerNames) {
			Enumeration<String> headerValues = request.getHeaders(headerName);
			while (headerValues != null && headerValues.hasMoreElements()) {
				values.addAll(splitDelimited(headerValues.nextElement()));
			}
		}
		return values;
	}

	private static Set<String> splitDelimited(String value) {
		if (value == null || value.isBlank()) {
			return Set.of();
		}
		Set<String> values = new LinkedHashSet<>();
		for (String token : value.split(",")) {
			String trimmed = token.trim();
			if (!trimmed.isEmpty()) {
				values.add(trimmed);
			}
		}
		return values;
	}

	private static <T> List<T> emptyIfNull(List<T> values) {
		return values == null ? Collections.emptyList() : values;
	}

	private static final class Bundle {
		private List<Policy> policies;
	}

	private static final class Policy {
		private List<Rule> rules;
	}

	private static final class Rule {
		private RuleTarget target;
		private List<String> actions;
		private String effect;
		private String expressionType;
		private String expression;
		private List<RuleSubject> subjects;
	}

	private static final class RuleTarget {
		private String datasetRef;
		private String namedGraph;
	}

	private static final class RuleSubject {
		private String type;
		private String value;
		private String claim;
	}
}