package integration

import (
	"fmt"
	"os"

	"github.com/newrelic/go-agent/v3/integrations/logcontext-v2/nrlogrus"
	"github.com/newrelic/go-agent/v3/newrelic"
	"github.com/newrelic/newrelic-labs-sdk/pkg/integration/log"
	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

func NewStandaloneIntegration(
	buildInfo *BuildInfo,
	appName string,
	labsIntegrationOpts ...LabsIntegrationOpt,
) (*LabsIntegration, error) {
	// Parse args
	parseStandaloneArgs()

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

	defer log.Debugf("starting %s integration", buildInfo.Name)

	// Create the integration
	return newLabsIntegration(
		buildInfo,
		app,
		nil,
		log.RootLogger,
		viper.GetBool("runAsService"),
		viper.GetBool("dry_run"),
		labsIntegrationOpts,
	)
}

func setupApm(
	appName string,
	logger *logrus.Logger,
) (*newrelic.Application, error) {
	// Look for our license key
	licenseKey := viper.GetString("licenseKey")
	if licenseKey == "" {
	  licenseKey = os.Getenv("NEW_RELIC_LICENSE_KEY")
	}

	// Get the appName to use
	apmAppName := appName
	if apmAppName == "" {
		apmAppName = viper.GetString("appName")
		if apmAppName == "" {
			apmAppName = os.Getenv("NEW_RELIC_APP_NAME")
			if apmAppName == "" {
				if os.Args[0] != "" {
					apmAppName = os.Args[0]
				} else {
					return nil, fmt.Errorf("no application name found")
				}
			}
		}
	}

	// We don't care if this fails, the Agent is nil safe
	app, _ := newrelic.NewApplication(
	  newrelic.ConfigAppName(apmAppName),
	  newrelic.ConfigLicense(licenseKey),
	)

	// Setup in context logging
	if app != nil {
	  logger.SetFormatter(nrlogrus.NewFormatter(app, &logrus.TextFormatter{}))
	}

	return app, nil
}

func parseStandaloneArgs() {
	pflag.Bool(
		"verbose",
		false,
		"enable verbose logging",
	)
	pflag.Bool(
		"dry_run",
		false,
		"run in dry run mode",
	)
	pflag.Bool(
		"version",
		false,
		"display version information",
	)
	pflag.String(
		"config_path",
		"",
		"path to YML configuration file",
	)
	pflag.String(
		"env_prefix",
		"",
		"prefix to use for environment variable lookup",
	)

	pflag.Parse()

	// Bind pflags to viper. This isn't done in the common setupConfig because
	// on the infrastructure side we don't bind pflags because flags are already
	// parsed in the infra code.

	viper.BindPFlags(pflag.CommandLine)
}
