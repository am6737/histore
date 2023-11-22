package filter

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	kubevirtv1 "kubevirt.io/api/core/v1"
	"testing"
)

func createVirtualMachineList() *kubevirtv1.VirtualMachineList {
	vm1 := kubevirtv1.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vm1",
			Namespace: "namespace1",
			Labels: map[string]string{
				"app": "app1",
			},
		},
	}
	vm2 := kubevirtv1.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vm2",
			Namespace: "namespace2",
			Labels: map[string]string{
				"app": "app2",
			},
		},
	}
	vm3 := kubevirtv1.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vm3",
			Namespace: "namespace2",
			Labels: map[string]string{
				"app": "app3",
			},
		},
	}
	vm4 := kubevirtv1.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vm4",
			Namespace: "namespace3",
			Labels: map[string]string{
				"app": "app2",
			},
		},
	}
	vm5 := kubevirtv1.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vm5",
			Namespace: "namespace3",
			Labels: map[string]string{
				"app": "app3",
			},
		},
	}
	vmList := &kubevirtv1.VirtualMachineList{}
	vmList.Items = append(vmList.Items, vm1, vm2, vm3, vm4, vm5)
	return vmList
}

func TestVirtualMachineFilter_Filter(t *testing.T) {
	vmList := createVirtualMachineList()

	testCases := []struct {
		name                     string
		includedNamespaces       []string
		excludedNamespaces       []string
		labelSelector            *metav1.LabelSelector
		orLabelSelectors         []*metav1.LabelSelector
		excludedLabelSelector    *metav1.LabelSelector
		excludedOrLabelSelectors []*metav1.LabelSelector
		expectedLength           int
	}{
		{
			name:               "IncludedNamespaces",
			includedNamespaces: []string{"namespace1", "namespace3"},
			expectedLength:     3,
		},
		{
			name:               "ExcludedNamespaces",
			excludedNamespaces: []string{"namespace2"},
			expectedLength:     3,
		},
		{
			name: "LabelSelector",
			labelSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "app1"},
			},
			expectedLength: 1,
		},
		{
			name: "OrLabelSelectors",
			orLabelSelectors: []*metav1.LabelSelector{
				{
					MatchLabels: map[string]string{"app": "app2"},
				},
				{
					MatchLabels: map[string]string{"app": "app3"},
				},
			},
			expectedLength: 4,
		},
		{
			name: "ExcludedLabelSelector",
			excludedLabelSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "app2"},
			},
			expectedLength: 3,
		},
		{
			name: "ExcludedOrLabelSelectors",
			excludedOrLabelSelectors: []*metav1.LabelSelector{
				{
					MatchLabels: map[string]string{"app": "app2"},
				},
				{
					MatchLabels: map[string]string{"app": "app3"},
				},
			},
			expectedLength: 1, // Excludes resources with "app=app2" or "app=app3"
		},
		{
			name:               "IncludedAndExcludedLabelSelector",
			includedNamespaces: []string{"namespace2"},
			excludedLabelSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "app2"},
			},
			expectedLength: 1, // Namespace included but label excluded
		},
		// Add more test cases here to cover other combinations
	}

	// Run tests for each test case
	for _, tc := range testCases {
		filter := &VirtualMachineFilter{
			IncludedNamespaces:       tc.includedNamespaces,
			ExcludedNamespaces:       tc.excludedNamespaces,
			LabelSelector:            tc.labelSelector,
			OrLabelSelectors:         tc.orLabelSelectors,
			ExcludedLabelSelector:    tc.excludedLabelSelector,
			ExcludedOrLabelSelectors: tc.excludedOrLabelSelectors,
		}

		var resources []*unstructured.Unstructured
		for i := range vmList.Items {
			vm := vmList.Items[i]
			obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&vm)
			if err != nil {
				t.Fatalf("Failed to convert to Unstructured: %v", err)
			}
			unstructuredObj := &unstructured.Unstructured{Object: obj}
			resources = append(resources, unstructuredObj)
		}

		filteredResources := filter.Filter(resources)

		if len(filteredResources) != tc.expectedLength {
			t.Errorf("%s: Expected %d resources after filtering, got %d", tc.name, tc.expectedLength, len(filteredResources))
		}
	}
}
