package integration

import (
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/log"
	"github.com/spf13/viper"
)

func NewLambdaIntegration(
	name, id, appName string,
	labsIntegrationOpts ...LabsIntegrationOpt,
) (*LabsIntegration, error) {
	// We don't setup/process command line flags since we are running as a
	// lambda.

	// Load configuration with viper
	// We have to do this prior to setting up the APM app because the license
	// key may be in the config.
	err := loadConfig()
	if err != nil {
		return nil, err
	}

	// Now that the config is loaded, setup logging
	err = setupLogging(log.RootLogger)
	if err != nil {
		return nil, err
	}

	// Setup APM
	app, err := setupApm(appName, log.RootLogger)
	if err != nil {
		return nil, err
	}

	defer log.Debugf("starting %s integration", name)

	// Create the integration
	return newLabsIntegration(
		name,
		id,
		app,
		nil,		// this is not an infra integration
		log.RootLogger,
		false,		// can't run as service when running as a lambda
		viper.GetBool("dry_run"),
		labsIntegrationOpts...,
	)
}
