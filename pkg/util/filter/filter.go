package filter

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
)

// ResourceFilter is the interface that needs to be implemented
type ResourceFilter interface {
	Filter(resources []*unstructured.Unstructured) []*unstructured.Unstructured
}

type VirtualMachineFilter struct {
	IncludedNamespaces []string                `json:"includedNamespaces,omitempty"`
	ExcludedNamespaces []string                `json:"excludedNamespaces,omitempty"`
	LabelSelector      *metav1.LabelSelector   `json:"labelSelector,omitempty"`
	OrLabelSelectors   []*metav1.LabelSelector `json:"orLabelSelectors,omitempty"`
}

func (f *VirtualMachineFilter) Filter(resources []*unstructured.Unstructured) []*unstructured.Unstructured {
	var filteredResources []*unstructured.Unstructured

	for _, resource := range resources {
		// Check if the resource belongs to an included namespace
		namespace := resource.GetNamespace()
		if !f.isNamespaceIncluded(namespace) {
			continue
		}

		// Check if the resource belongs to an excluded namespace
		if f.isNamespaceExcluded(namespace) {
			continue
		}

		// Check if the resource matches the label selector
		if f.LabelSelector != nil && !f.matchesLabelSelector(resource) {
			continue
		}

		// Check if the resource matches any of the OR label selectors
		if !f.matchesOrLabelSelectors(resource) {
			continue
		}

		// Add the resource to the filtered list
		filteredResources = append(filteredResources, resource)
	}

	return filteredResources
}

func (f *VirtualMachineFilter) isNamespaceIncluded(namespace string) bool {
	if len(f.IncludedNamespaces) == 0 {
		return true
	}

	for _, includedNamespace := range f.IncludedNamespaces {
		if includedNamespace == namespace {
			return true
		}
	}

	return false
}

func (f *VirtualMachineFilter) isNamespaceExcluded(namespace string) bool {
	for _, excludedNamespace := range f.ExcludedNamespaces {
		if excludedNamespace == namespace {
			return true
		}
	}

	return false
}

func (f *VirtualMachineFilter) matchesLabelSelector(resource *unstructured.Unstructured) bool {
	if f.LabelSelector == nil {
		return true
	}

	selector, err := metav1.LabelSelectorAsSelector(f.LabelSelector)
	if err != nil {
		return false
	}

	return selector.Matches(labels.Set(resource.GetLabels()))
}

func (f *VirtualMachineFilter) matchesOrLabelSelectors(resource *unstructured.Unstructured) bool {
	if f.OrLabelSelectors == nil {
		return true
	}

	for _, selector := range f.OrLabelSelectors {
		selector, err := metav1.LabelSelectorAsSelector(selector)
		if err != nil {
			continue
		}

		if selector.Matches(labels.Set(resource.GetLabels())) {
			return true
		}
	}

	return false
}
