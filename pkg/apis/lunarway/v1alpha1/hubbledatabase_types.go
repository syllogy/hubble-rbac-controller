package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// HubbleDatabaseSpec defines the desired state of HubbleDatabase
type HubbleDatabaseSpec struct {
	Name string `json:"name"`
	Cluster string `json:"cluster"`
	Database string `json:"database"`
}

// HubbleDatabaseStatus defines the observed state of HubbleDatabase
type HubbleDatabaseStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "operator-sdk generate k8s" to regenerate code after modifying this file
	// Add custom validation using kubebuilder tags: https://book-v1.book.kubebuilder.io/beyond_basics/generating_crd.html
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// HubbleDatabase is the Schema for the hubbledatabases API
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=hubbledatabases,scope=Namespaced
type HubbleDatabase struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   HubbleDatabaseSpec   `json:"spec,omitempty"`
	Status HubbleDatabaseStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// HubbleDatabaseList contains a list of HubbleDatabase
type HubbleDatabaseList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []HubbleDatabase `json:"items"`
}

func init() {
	SchemeBuilder.Register(&HubbleDatabase{}, &HubbleDatabaseList{})
}
