import org.apache.jena.fuseki.main.FusekiMain;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.fuseki.main.sys.FusekiModules;
import org.apache.jena.fuseki.mod.admin.FMod_Admin;
import org.apache.jena.fuseki.mod.system.FMod_Ping;

import jakarta.servlet.Filter;

public final class FusekiHttpsLauncher {
	private FusekiHttpsLauncher() {}

	public static void main(String[] args) throws Exception {
		FusekiModules modules = FusekiModules.create(
			FMod_Admin.create(),
			FMod_Ping.create()
		);

		FusekiServer.Builder builder = FusekiMain.builder(modules, args);
		if (usesHttps(args) && !setsHttpPort(args)) {
			builder.port(-1);
		}

		Filter authorizationFilter = FusekiAuthorizationFilter.createFromEnvironment();
		if (authorizationFilter != null) {
			builder.addFilter("/*", authorizationFilter);
		}

		FusekiServer server = builder.build();
		Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
		server.start();
		server.getJettyServer().join();
	}

	private static boolean usesHttps(String[] args) {
		for (String arg : args) {
			if (arg.startsWith("--https=") || arg.equals("--https") || arg.startsWith("--httpsPort=")) {
				return true;
			}
		}
		return false;
	}

	private static boolean setsHttpPort(String[] args) {
		for (String arg : args) {
			if (arg.startsWith("--port=") || arg.equals("--port")) {
				return true;
			}
		}
		return false;
	}
}