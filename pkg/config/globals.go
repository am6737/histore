package config

import (
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func SetGlobals(client client.Client, cfg *CephConfig) error {

	//msc := &storagev1.StorageClass{}
	//if err := client.Get(context.TODO(), types.NamespacedName{Name: cfg.MasterStorageClass}, msc); err != nil {
	//	return err
	//}
	//DC.MasterCephCsiCfg = NewCephCsiConfig(msc)
	//
	//ssc := &storagev1.StorageClass{}
	//if err := client.Get(context.TODO(), types.NamespacedName{Name: cfg.MasterStorageClass}, ssc); err != nil {
	//	return err
	//}
	//DC.SlaveCephCsiCfg = NewCephCsiConfig(ssc)

	DC = cfg
	//DC.MasterStorageClass = cfg.MasterStorageClass
	//DC.SlaveStorageClass = cfg.SlaveStorageClass

	return nil
}

var (
	DC          *CephConfig
	SlavePoolID int64
)
