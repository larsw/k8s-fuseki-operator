package main

import (
	"flag"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	fusekiv1alpha1 "github.com/larsw/k8s-fuseki-operator/api/v1alpha1"
	fusekicontroller "github.com/larsw/k8s-fuseki-operator/internal/controller"
	"github.com/larsw/k8s-fuseki-operator/pkg/version"
)

var scheme = runtimeScheme()

func runtimeScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(s))
	utilruntime.Must(fusekiv1alpha1.AddToScheme(s))
	return s
}

func main() {
	var metricsAddr string
	var probeAddr string
	var enableLeaderElection bool

	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "Address for the metrics endpoint.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "Address for health and readiness probes.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false, "Enable leader election for the manager.")
	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: metricsAddr,
		},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "fuseki-operator-manager.fuseki.apache.org",
	})
	if err != nil {
		ctrl.Log.WithName("setup").Error(err, "unable to start manager", "version", version.String())
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		ctrl.Log.WithName("setup").Error(err, "unable to register healthz check")
		os.Exit(1)
	}

	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		ctrl.Log.WithName("setup").Error(err, "unable to register readyz check")
		os.Exit(1)
	}

	if err := fusekicontroller.SetupV020Webhooks(mgr); err != nil {
		ctrl.Log.WithName("setup").Error(err, "unable to create validating webhooks")
		os.Exit(1)
	}

	if err := (&fusekicontroller.FusekiClusterReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		ctrl.Log.WithName("setup").Error(err, "unable to create controller", "controller", "FusekiCluster")
		os.Exit(1)
	}

	if err := (&fusekicontroller.FusekiServerReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		ctrl.Log.WithName("setup").Error(err, "unable to create controller", "controller", "FusekiServer")
		os.Exit(1)
	}

	if err := (&fusekicontroller.RDFDeltaServerReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		ctrl.Log.WithName("setup").Error(err, "unable to create controller", "controller", "RDFDeltaServer")
		os.Exit(1)
	}

	if err := (&fusekicontroller.DatasetReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		ctrl.Log.WithName("setup").Error(err, "unable to create controller", "controller", "Dataset")
		os.Exit(1)
	}

	if err := (&fusekicontroller.SecurityProfileReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		ctrl.Log.WithName("setup").Error(err, "unable to create controller", "controller", "SecurityProfile")
		os.Exit(1)
	}

	if err := (&fusekicontroller.SecurityPolicyReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		ctrl.Log.WithName("setup").Error(err, "unable to create controller", "controller", "SecurityPolicy")
		os.Exit(1)
	}

	if err := (&fusekicontroller.SHACLPolicyReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		ctrl.Log.WithName("setup").Error(err, "unable to create controller", "controller", "SHACLPolicy")
		os.Exit(1)
	}

	if err := (&fusekicontroller.ImportRequestReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		ctrl.Log.WithName("setup").Error(err, "unable to create controller", "controller", "ImportRequest")
		os.Exit(1)
	}

	if err := (&fusekicontroller.ExportRequestReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		ctrl.Log.WithName("setup").Error(err, "unable to create controller", "controller", "ExportRequest")
		os.Exit(1)
	}

	if err := (&fusekicontroller.IngestPipelineReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		ctrl.Log.WithName("setup").Error(err, "unable to create controller", "controller", "IngestPipeline")
		os.Exit(1)
	}

	if err := (&fusekicontroller.ChangeSubscriptionReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		ctrl.Log.WithName("setup").Error(err, "unable to create controller", "controller", "ChangeSubscription")
		os.Exit(1)
	}

	if err := (&fusekicontroller.BackupPolicyReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		ctrl.Log.WithName("setup").Error(err, "unable to create controller", "controller", "BackupPolicy")
		os.Exit(1)
	}

	if err := (&fusekicontroller.RestoreRequestReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		ctrl.Log.WithName("setup").Error(err, "unable to create controller", "controller", "RestoreRequest")
		os.Exit(1)
	}

	if err := (&fusekicontroller.EndpointReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		ctrl.Log.WithName("setup").Error(err, "unable to create controller", "controller", "Endpoint")
		os.Exit(1)
	}

	if err := (&fusekicontroller.FusekiUIReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		ctrl.Log.WithName("setup").Error(err, "unable to create controller", "controller", "FusekiUI")
		os.Exit(1)
	}

	ctrl.Log.WithName("setup").Info("starting manager", "version", version.String())
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		ctrl.Log.WithName("setup").Error(err, "manager exited")
		os.Exit(1)
	}
}
