package integration

import (
	"github.com/spf13/viper"
)

func NewConfigWithFile(configFile string) error {
	viper.SetConfigFile(configFile)

	return newConfig()
}

func NewConfigWithPaths() error {
	viper.SetConfigName("config")
	viper.AddConfigPath("configs")
	viper.AddConfigPath(".")

	return newConfig()
}

func newConfig() error {
	err := viper.ReadInConfig()
	if err != nil {
		return err
	}

	return nil
}
