package integration

import (
	"github.com/spf13/viper"
)

func NewConfigWithFile(configFile string, envPrefix string) error {
	viper.SetConfigFile(configFile)

	return newConfig(envPrefix)
}

func NewConfigWithPaths(envPrefix string) error {
	viper.SetConfigName("config")
	viper.AddConfigPath("configs")
	viper.AddConfigPath(".")

	return newConfig(envPrefix)
}

func newConfig(envPrefix string) error {
	viper.AutomaticEnv()
	if envPrefix != "" {
		viper.SetEnvPrefix(envPrefix)
	}

	err := viper.ReadInConfig()
	if err != nil {
		return err
	}

	/*
	@TODO incorporate this when incorporating a scheduler
	interval := viper.GetUint("interval")
	if interval <= 0 {
		interval = 60
	}
	*/

	return nil
}
