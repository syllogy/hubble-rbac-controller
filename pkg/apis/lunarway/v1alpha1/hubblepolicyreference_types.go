package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// HubblePolicyReferenceSpec defines the desired state of HubblePolicyReference
type HubblePolicyReferenceSpec struct {
	Name string `json:"name"`
	Arn string `json:"arn"`
}

// HubblePolicyReferenceStatus defines the observed state of HubblePolicyReference
type HubblePolicyReferenceStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "operator-sdk generate k8s" to regenerate code after modifying this file
	// Add custom validation using kubebuilder tags: https://book-v1.book.kubebuilder.io/beyond_basics/generating_crd.html
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// HubblePolicyReference is the Schema for the hubblepolicyreferences API
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=hubblepolicyreferences,scope=Namespaced
type HubblePolicyReference struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   HubblePolicyReferenceSpec   `json:"spec,omitempty"`
	Status HubblePolicyReferenceStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// HubblePolicyReferenceList contains a list of HubblePolicyReference
type HubblePolicyReferenceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []HubblePolicyReference `json:"items"`
}

func init() {
	SchemeBuilder.Register(&HubblePolicyReference{}, &HubblePolicyReferenceList{})
}
