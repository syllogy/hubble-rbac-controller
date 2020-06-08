package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.


// HubbleUserSpec defines the desired state of HubbleUser
type HubbleUserSpec struct {
	Name string `json:"name"`
	Email string `json:"email"`
	Roles []string `json:"roles"`
}

// HubbleUserStatus defines the observed state of HubbleUser
type HubbleUserStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "operator-sdk generate k8s" to regenerate code after modifying this file
	// Add custom validation using kubebuilder tags: https://book-v1.book.kubebuilder.io/beyond_basics/generating_crd.html
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// HubbleUser is the Schema for the hubbleusers API
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=hubbleusers,scope=Namespaced
type HubbleUser struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   HubbleUserSpec   `json:"spec,omitempty"`
	Status HubbleUserStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// HubbleUserList contains a list of HubbleUser
type HubbleUserList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []HubbleUser `json:"items"`
}

func init() {
	SchemeBuilder.Register(&HubbleUser{}, &HubbleUserList{})
}
