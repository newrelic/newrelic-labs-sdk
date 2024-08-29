package integration

import (
	nriSdkArgs "github.com/newrelic/infra-integrations-sdk/v4/args"
	nriSdk "github.com/newrelic/infra-integrations-sdk/v4/integration"
	nriSdkLog "github.com/newrelic/infra-integrations-sdk/v4/log"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/build"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/log"
	"github.com/spf13/viper"
)

type InfraIntegrationArgs struct {
	nriSdkArgs.DefaultArgumentList
	ConfigPath	string 	`help:"Path to configuration"`
	ShowVersion	bool   	`default:"false" help:"Print build information and exit"`
	EnvPrefix	string 	`default:"" help:"Prefix to use for environment variable lookup"`
}

var (
	infraArgs	InfraIntegrationArgs
)

func NewInfraIntegration(
	name, id string,
	labsIntegrationOpts ...LabsIntegrationOpt,
) (*LabsIntegration, error) {
	// Create the native infra integration
	i, err := createInfraIntegration(id, log.RootLogger)
	if err != nil {
		return nil, err
	}

	// Maybe show the version info
	if infraArgs.ShowVersion {
		showVersionAndExit(name)
	}

	// Bind custom flag information to viper
	// We have to do this manually because infra already parses flags and we
	// don't want to reparse them with viper/pflag.
	bindFlags()

	// Load configuration with viper
	err = loadConfig()
	if err != nil {
		return nil, err
	}

	// Now that the config is loaded, setup logging
	err = setupLogging(log.RootLogger)
	if err != nil {
		return nil, err
	}

	defer log.Debugf("starting %s integration", name)

	return newLabsIntegration(
		name,
		id,
		nil,
		i,
		log.RootLogger,
		false,
		false,
		labsIntegrationOpts,
	)
}

func createInfraIntegration(
	id string,
	logger nriSdkLog.Logger,
) (*nriSdk.Integration, error) {
	return nriSdk.New(
		id,
		build.GetBuildInfo().Version,
		nriSdk.Args(&infraArgs),
		nriSdk.Logger(logger),
	)
}

func bindFlags() {
	viper.Set("verbose", infraArgs.Verbose)
	viper.Set("version", infraArgs.ShowVersion)
	viper.Set("config_path", infraArgs.ConfigPath)
	viper.Set("env_prefix", infraArgs.EnvPrefix)
}
