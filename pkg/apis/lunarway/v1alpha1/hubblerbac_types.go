package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// HubbleRbacSpec defines the desired state of HubbleRbac
type HubbleRbacSpec struct {
	Users []User `json:"users"`
	Roles []Role `json:"roles"`
	Policies []PolicyReference `json:"policies"`
	Databases []Database `json:"databases"`
	DevDatabases []DeveloperDatabase `json:"devDatabases"`
}

type User struct {
	Name string `json:"name"`
	Email string `json:"email"`
	Roles []string `json:"roles"`
}

type Role struct {
	Name string `json:"name"`
	Databases []string `json:"databases"`
	DevDatabases []string `json:"devDatabases"`
	DatalakeGrants []string `json:"datalakeGrants"`
	DatawarehouseGrants []string `json:"datawarehouseGrants"`
	Policies []string `json:"policies"`
}

type PolicyReference struct {
	Name string `json:"name"`
	Arn string `json:"arn"`
}

type DeveloperDatabase struct {
	Name string `json:"name"`
	Cluster string `json:"cluster"`
}

type Database struct {
	Name string `json:"name"`
	Cluster string `json:"cluster"`
	Database string `json:"database"`
}

// HubbleRbacStatus defines the observed state of HubbleRbac
type HubbleRbacStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "operator-sdk generate k8s" to regenerate code after modifying this file
	// Add custom validation using kubebuilder tags: https://book-v1.book.kubebuilder.io/beyond_basics/generating_crd.html
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// HubbleRbac is the Schema for the hubblerbacs API
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=hubblerbacs,scope=Namespaced
type HubbleRbac struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   HubbleRbacSpec   `json:"spec,omitempty"`
	Status HubbleRbacStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// HubbleRbacList contains a list of HubbleRbac
type HubbleRbacList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []HubbleRbac `json:"items"`
}

func init() {
	SchemeBuilder.Register(&HubbleRbac{}, &HubbleRbacList{})
}
