import org.apache.jena.fuseki.main.FusekiMain;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.fuseki.main.sys.FusekiModules;
import org.apache.jena.fuseki.mod.admin.FMod_Admin;
import org.apache.jena.fuseki.mod.system.FMod_Ping;

public final class FusekiHttpsLauncher {
	private FusekiHttpsLauncher() {}

	public static void main(String[] args) throws Exception {
		FusekiModules modules = FusekiModules.create(
			FMod_Admin.create(),
			FMod_Ping.create()
		);

		FusekiServer server = FusekiMain.builder(modules, args)
			.port(-1)
			.build();
		Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
		server.start();
		server.getJettyServer().join();
	}
}