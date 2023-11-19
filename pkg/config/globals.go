package config

func SetGlobals(cfg *CephConfig) {
	DC = cfg
}

var (
	DC          *CephConfig
	SlavePoolID int64
)

var MasterCephCsiCfg *CephCsiConfig
var SlaveCephCsiCfg *CephCsiConfig
