package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// HubbleRoleSpec defines the desired state of HubbleRole
type HubbleRoleSpec struct {
	Name string `json:"name"`
	Databases []string `json:"databases"`
	DevDatabases []string `json:"devDatabases"`
	DatalakeGrants []string `json:"datalakeGrants"`
	DatawarehouseGrants []string `json:"datawarehouseGrants"`
	Policies []string `json:"policies"`
}

// HubbleRoleStatus defines the observed state of HubbleRole
type HubbleRoleStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "operator-sdk generate k8s" to regenerate code after modifying this file
	// Add custom validation using kubebuilder tags: https://book-v1.book.kubebuilder.io/beyond_basics/generating_crd.html
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// HubbleRole is the Schema for the hubbleroles API
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=hubbleroles,scope=Namespaced
type HubbleRole struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   HubbleRoleSpec   `json:"spec,omitempty"`
	Status HubbleRoleStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// HubbleRoleList contains a list of HubbleRole
type HubbleRoleList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []HubbleRole `json:"items"`
}

func init() {
	SchemeBuilder.Register(&HubbleRole{}, &HubbleRoleList{})
}
