package config

func SetGlobals(cfg *DriverConfig) {
	DC = cfg
}

var (
	DC *DriverConfig
)
