package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// HubbleDeveloperDatabaseSpec defines the desired state of HubbleDeveloperDatabase
type HubbleDeveloperDatabaseSpec struct {
	Name string `json:"name"`
	Cluster string `json:"cluster"`
}

// HubbleDeveloperDatabaseStatus defines the observed state of HubbleDeveloperDatabase
type HubbleDeveloperDatabaseStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "operator-sdk generate k8s" to regenerate code after modifying this file
	// Add custom validation using kubebuilder tags: https://book-v1.book.kubebuilder.io/beyond_basics/generating_crd.html
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// HubbleDeveloperDatabase is the Schema for the hubbledeveloperdatabases API
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=hubbledeveloperdatabases,scope=Namespaced
type HubbleDeveloperDatabase struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   HubbleDeveloperDatabaseSpec   `json:"spec,omitempty"`
	Status HubbleDeveloperDatabaseStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// HubbleDeveloperDatabaseList contains a list of HubbleDeveloperDatabase
type HubbleDeveloperDatabaseList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []HubbleDeveloperDatabase `json:"items"`
}

func init() {
	SchemeBuilder.Register(&HubbleDeveloperDatabase{}, &HubbleDeveloperDatabaseList{})
}
