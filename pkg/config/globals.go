package config

func SetGlobals(cfg *CephConfig) {
	DC = cfg
}

var (
	DC *CephConfig
)

var MasterCephCsiCfg *CephCsiConfig
var SlaveCephCsiCfg *CephCsiConfig
