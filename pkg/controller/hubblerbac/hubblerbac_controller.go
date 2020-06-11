package hubblerbac

import (
	"context"
	"github.com/go-logr/logr"
	"github.com/lunarway/hubble-rbac-controller/internal/infrastructure/redshift"
	"github.com/lunarway/hubble-rbac-controller/internal/infrastructure/service"

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

/**
* USER ACTION REQUIRED: This is a scaffold file intended for the user to modify with their own Controller
* business logic.  Delete these comments after modifying this file.*
 */

//var ServiceAccountFilePath = os.Getenv("GOOGLE_CREDENTIALS_FILE_PATH")
const accountId = "478824949770"
const region = "eu-west-1"
var localhostCredentials redshift.ClusterCredentials

func init() {
	localhostCredentials = redshift.ClusterCredentials{
		Username:                 "lunarway",
		Password:                 "lunarway",
		MasterDatabase:           "lunarway",
		Host:                     "localhost",
		Sslmode:                  "disable",
		Port:                     5432,
		ExternalSchemasSupported: false,
	}
}


// Add creates a new HubbleRbac Controller and adds it to the Manager. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager) error {
	excludedUsers := []string{
		"lunarway",
	}
	//clientGroup := redshift.NewClientGroup(map[string]*redshift.ClusterCredentials{"hubble": &localhostCredentials})
	//
	//session := iam.LocalStackSessionFactory{}.CreateSession()
	//iamClient := iam.New(session)
	//
	//jsonCredentials, err := ioutil.ReadFile(ServiceAccountFilePath)
	//if err != nil {
	//	return fmt.Errorf("unable to load google credentials: %v", err)
	//}
	//googleClient, err := google.NewGoogleClient(jsonCredentials, "jwr@chatjing.com", accountId)
	//if err != nil {
	//	return fmt.Errorf("unable to initialize google client: %v", err)
	//}

	//applier := service.NewApplier(clientGroup, iamClient, googleClient, excludedUsers, accountId, region)
	applier := service.NewApplier(nil, nil, nil, excludedUsers, accountId, region, log)

	return add(mgr, newReconciler(mgr, applier))
}

func newReconciler(mgr manager.Manager, applier *service.Applier) reconcile.Reconciler {
	return &ReconcileHubbleRbac{client: mgr.GetClient(), scheme: mgr.GetScheme(), applier: applier}
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

// ReconcileHubbleRbac reconciles a HubbleRbac object
type ReconcileHubbleRbac struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	client client.Client
	scheme *runtime.Scheme
	applier *service.Applier
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


// Reconcile reads that state of the cluster for a HubbleRbac object and makes changes based on the state read
// and what is in the HubbleRbac.Spec
// TODO(user): Modify this Reconcile function to implement your Controller logic.  This example creates
// a Pod as an example
// Note:
// The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
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

	err = r.applier.Apply(model,true)
	if err != nil {
		r.setStatusFailed(instance, err, reqLogger)
		return reconcile.Result{}, err
	}

	r.setStatusOk(instance, reqLogger)

	return reconcile.Result{}, nil
}
