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

package controllers

import (
	"context"
	"github.com/go-logr/logr"
	"github.com/lunarway/hubble-rbac-controller/internal/infrastructure/service"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	hubblev1alpha1 "github.com/lunarway/hubble-rbac-controller/api/v1alpha1"
)

var log = logf.Log.WithName("controller_hubblerbac")

// HubbleRbacReconciler reconciles a HubbleRbac object
type HubbleRbacReconciler struct {
	client.Client
	Log     logr.Logger
	Scheme  *runtime.Scheme
	Applier *service.Applier
	DryRun  bool
}

// +kubebuilder:rbac:groups=hubble.lunar.tech,resources=hubblerbacs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=hubble.lunar.tech,resources=hubblerbacs/status,verbs=get;update;patch

func (r *HubbleRbacReconciler) setStatusFailed(instance *hubblev1alpha1.HubbleRbac, err error, logger logr.Logger) {
	instance.Status.Error = err.Error()
	statusUpdateError := r.Status().Update(context.TODO(), instance)

	if statusUpdateError != nil {
		logger.Error(statusUpdateError, "unable to update status")
	}
}

func (r *HubbleRbacReconciler) setStatusOk(instance *hubblev1alpha1.HubbleRbac, logger logr.Logger) {
	instance.Status.Error = ""
	statusUpdateError := r.Status().Update(context.TODO(), instance)

	if statusUpdateError != nil {
		logger.Error(statusUpdateError, "unable to update status")
	}
}

func (r *HubbleRbacReconciler) Reconcile(request ctrl.Request) (ctrl.Result, error) {
	_ = context.Background()
	_ = r.Log.WithValues("hubblerbac", request.NamespacedName)

	// your logic here
	instance := &hubblev1alpha1.HubbleRbac{}

	err := r.Get(context.TODO(), request.NamespacedName, instance)
	if err != nil {
		if errors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}

	model, err := buildHubbleModel(instance)
	if err != nil {
		r.Log.Error(err, "invalid HubbleRbac CR encountered")
		r.setStatusFailed(instance, err, r.Log)
		return reconcile.Result{}, nil //don't reschedule, if we can't construct the hubble model from the CR it is a permanent problem
	}

	err = r.Applier.Apply(model, r.DryRun)
	if err != nil {
		r.setStatusFailed(instance, err, r.Log)
		return reconcile.Result{}, err
	}

	r.setStatusOk(instance, r.Log)

	return ctrl.Result{}, nil
}

func (r *HubbleRbacReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&hubblev1alpha1.HubbleRbac{}).
		Complete(r)
}
