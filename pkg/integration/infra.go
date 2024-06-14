package integration

import (
	nriSdkArgs "github.com/newrelic/infra-integrations-sdk/v4/args"
	nriSdk "github.com/newrelic/infra-integrations-sdk/v4/integration"
	nriSdkLog "github.com/newrelic/infra-integrations-sdk/v4/log"
	"github.com/newrelic/newrelic-labs-sdk/pkg/integration/log"
)

type InfraIntegrationArgs struct {
	nriSdkArgs.DefaultArgumentList
	ConfigPath  string `help:"Path to configuration"`
	ShowVersion bool   `default:"false" help:"Print build information and exit"`
}

var (
	infraArgs	InfraIntegrationArgs
)

func NewInfraIntegration(
	buildInfo *BuildInfo,
	envPrefix string,
	labsIntegrationOpts ...LabsIntegrationOpt,
) (*LabsIntegration, error) {
	// Create the native infra integration
	i, err := createInfraIntegration(buildInfo, log.RootLogger)
	if err != nil {
		return nil, err
	}

	// Load configuration with viper
	err = loadConfig(infraArgs.ConfigPath, envPrefix)
	if err != nil {
		return nil, err
	}

	// Now that the config is loaded, setup logging
	err = setupLogging(log.RootLogger, infraArgs.Verbose)
	if err != nil {
		return nil, err
	}

	defer log.Debugf("starting %s integration", buildInfo.Name)

	return newLabsIntegration(
		buildInfo,
		nil,
		i,
		log.RootLogger,
		false,
		false,
		labsIntegrationOpts,
	)
}

func createInfraIntegration(
	buildInfo *BuildInfo,
	logger nriSdkLog.Logger,
) (*nriSdk.Integration, error) {
	return nriSdk.New(
		buildInfo.Id,
		buildInfo.Version,
		nriSdk.Args(&infraArgs),
		nriSdk.Logger(logger),
	)
}
