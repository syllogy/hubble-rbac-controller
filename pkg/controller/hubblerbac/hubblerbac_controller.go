package hubblerbac

import (
	"context"
	"fmt"
	"github.com/go-logr/logr"
	redshiftCore "github.com/lunarway/hubble-rbac-controller/internal/core/redshift"
	"github.com/lunarway/hubble-rbac-controller/internal/infrastructure/google"
	"github.com/lunarway/hubble-rbac-controller/internal/infrastructure/iam"
	"github.com/lunarway/hubble-rbac-controller/internal/infrastructure/redshift"
	"github.com/lunarway/hubble-rbac-controller/internal/infrastructure/service"
	"github.com/lunarway/hubble-rbac-controller/pkg/configuration"
	"io/ioutil"

	lunarwayv1alpha1 "github.com/lunarway/hubble-rbac-controller/pkg/apis/lunarway/v1alpha1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

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
	redshiftApplier := redshift.NewApplier(clientGroup, redshiftCore.NewExclusions(excludedDatabases, excludedUsers), conf.AwsAccountId, log, config)

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

func Add(mgr manager.Manager) error {

	conf, err := configuration.LoadConfiguration()

	if err != nil {
		return fmt.Errorf("unable to load configuration: %w", err)
	}

	applier, err := createApplier(conf)

	if err != nil {
		return fmt.Errorf("unable to create applier: %w", err)
	}

	return add(mgr, newReconciler(mgr, applier, conf.DryRun))
}

func newReconciler(mgr manager.Manager, applier *service.Applier, dryRun bool) reconcile.Reconciler {
	return &ReconcileHubbleRbac{client: mgr.GetClient(), scheme: mgr.GetScheme(), applier: applier, dryRun: dryRun}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {

	c, err := controller.New("hubblerbac-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to primary resource HubbleRbac
	err = c.Watch(&source.Kind{Type: &lunarwayv1alpha1.HubbleRbac{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	return nil
}

// blank assignment to verify that ReconcileHubbleRbac implements reconcile.Reconciler
var _ reconcile.Reconciler = &ReconcileHubbleRbac{}

type ReconcileHubbleRbac struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	client  client.Client
	scheme  *runtime.Scheme
	applier *service.Applier
	dryRun  bool
}

func (r *ReconcileHubbleRbac) setStatusFailed(instance *lunarwayv1alpha1.HubbleRbac, err error, logger logr.Logger) {
	instance.Status.Error = err.Error()
	statusUpdateError := r.client.Status().Update(context.TODO(), instance)

	if statusUpdateError != nil {
		logger.Error(statusUpdateError, "unable to update status")
	}
}

func (r *ReconcileHubbleRbac) setStatusOk(instance *lunarwayv1alpha1.HubbleRbac, logger logr.Logger) {
	instance.Status.Error = ""
	statusUpdateError := r.client.Status().Update(context.TODO(), instance)

	if statusUpdateError != nil {
		logger.Error(statusUpdateError, "unable to update status")
	}
}

func (r *ReconcileHubbleRbac) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	reqLogger := log.WithValues("Request.Namespace", request.Namespace, "Request.Name", request.Name)
	reqLogger.Info("Reconciling HubbleModel")

	instance := &lunarwayv1alpha1.HubbleRbac{}
	err := r.client.Get(context.TODO(), request.NamespacedName, instance)
	if err != nil {
		if errors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}

	model, err := buildHubbleModel(instance)
	if err != nil {
		reqLogger.Error(err, "invalid HubbleRbac CR encountered")
		r.setStatusFailed(instance, err, reqLogger)
		return reconcile.Result{}, nil //don't reschedule, if we can't construct the hubble model from the CR it is a permanent problem
	}

	err = r.applier.Apply(model, r.dryRun)
	if err != nil {
		r.setStatusFailed(instance, err, reqLogger)
		return reconcile.Result{}, err
	}

	r.setStatusOk(instance, reqLogger)

	return reconcile.Result{}, nil
}
