package controller

import (
	"context"
	"github.com/am6737/histore/pkg/config"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func getCephCsiConfigForSC(client client.Client, scName string) (*config.CephCsiConfig, error) {
	sc := &storagev1.StorageClass{}
	if err := client.Get(context.TODO(), types.NamespacedName{Name: scName}, sc); err != nil {
		return nil, err
	}
	return config.NewCephCsiConfig(sc), nil
}
