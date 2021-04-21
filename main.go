/*


Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"flag"
	"fmt"
	"github.com/lunarway/hubble-rbac-controller/internal/infrastructure/google"
	"github.com/lunarway/hubble-rbac-controller/internal/infrastructure/iam"
	"github.com/lunarway/hubble-rbac-controller/internal/infrastructure/redshift"
	"github.com/lunarway/hubble-rbac-controller/internal/infrastructure/service"
	"github.com/lunarway/hubble-rbac-controller/pkg/configuration"
	"io/ioutil"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	hubblev1alpha1 "github.com/lunarway/hubble-rbac-controller/api/v1alpha1"
	"github.com/lunarway/hubble-rbac-controller/controllers"
	// +kubebuilder:scaffold:imports

	redshiftCore "github.com/lunarway/hubble-rbac-controller/internal/core/redshift"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(hubblev1alpha1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

var log = logf.Log.WithName("controller_hubblerbac")

func createApplier(conf configuration.Configuration) (*service.Applier, error) {

	excludedUsers := []string{
		"produser",
		"devuser",
		"dev",
		"inspari",
		"looker",
		"rdsdb",
	}
	//these databases come baked into a redshift cluster, and we don't want to manage those
	excludedDatabases := []string{"template0", "template1", "postgres", "padb_harvest"}

	redshiftCredentials := redshift.ClusterCredentials{
		Username:                 conf.RedshiftUsername,
		Password:                 conf.RedshiftPassword,
		MasterDatabase:           conf.RedshiftMasterDatabase,
		Host:                     conf.RedshiftHostTemplate,
		Sslmode:                  "require",
		Port:                     5439,
		ExternalSchemasSupported: true,
	}

	clientGroup := redshift.NewClientGroup(&redshiftCredentials)

	//for some reason revoking access to the public schema in Redshift has no effect, so every reconcile would try to revoke access to all public schemas (so we skip it)
	config := redshiftCore.ReconcilerConfig{RevokeAccessToPublicSchema: false}
	redshiftApplier := redshift.NewApplier(clientGroup, redshiftCore.NewExclusions(excludedDatabases, excludedUsers), conf.AwsAccountId, log, config, conf.ExternalSchemas)

	session := iam.AwsSessionFactory{}.CreateSession()
	iamClient := iam.New(session)
	iamApplier := iam.NewApplier(iamClient, conf.AwsAccountId, conf.Region, service.NewIamLogger(log), log)

	jsonCredentials, err := ioutil.ReadFile(conf.GoogleCredentials)
	if err != nil {
		return nil, fmt.Errorf("unable to load google credentials: %v", err)
	}
	googleClient, err := google.NewGoogleClient(jsonCredentials, conf.GoogleAdminPrincipalEmail, conf.AwsAccountId)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize google client: %v", err)
	}
	googleApplier := google.NewApplier(googleClient)

	applier := service.NewApplier(iamApplier, googleApplier, redshiftApplier, log)

	return applier, nil
}

func main() {
	var metricsAddr string
	var enableLeaderElection bool
	flag.StringVar(&metricsAddr, "metrics-addr", ":8080", "The address the metric endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "enable-leader-election", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:             scheme,
		MetricsBindAddress: metricsAddr,
		Port:               9443,
		LeaderElection:     enableLeaderElection,
		LeaderElectionID:   "113ffd08.lunar.tech",
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	conf, err := configuration.LoadConfiguration()

	if err != nil {
		setupLog.Error(err, "unable to load configuration")
	}

	applier, err := createApplier(conf)

	if err != nil {
		setupLog.Error(err, "unable to create applier")
	}

	if err = (&controllers.HubbleRbacReconciler{
		Client:  mgr.GetClient(),
		Log:     ctrl.Log.WithName("controllers").WithName("HubbleRbac"),
		Scheme:  mgr.GetScheme(),
		Applier: applier,
		DryRun:  conf.DryRun,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "HubbleRbac")
		os.Exit(1)
	}
	// +kubebuilder:scaffold:builder

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
