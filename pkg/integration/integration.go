package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/newrelic/go-agent/v3/newrelic"
	nriSdk "github.com/newrelic/infra-integrations-sdk/v4/integration"
	nrClient "github.com/newrelic/newrelic-client-go/newrelic"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/build"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/log"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/pipeline"

	"github.com/newrelic/newrelic-client-go/pkg/config"
	"github.com/newrelic/newrelic-client-go/pkg/logging"
	"github.com/newrelic/newrelic-client-go/pkg/region"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	DEFAULT_INTERVAL = 60
)

type (
	LabsIntegrationOpt func(li *LabsIntegration) error
)

// @TODO: connectors Request() methods should take a context
// @TODO: support custom tags via config
// @TODO: connectors should take a Context

type LabsIntegration struct {
	Name			string
	Id				string
	BuildInfo		build.BuildInfo
	App           	*newrelic.Application
	Integration   	*nriSdk.Integration
	Logger        	*logrus.Logger
	Interval	  	time.Duration
	NrClient      	*nrClient.NewRelic
	RunAsService  	bool
	DryRun			bool
	pipelines     	[]pipeline.Pipeline
	apiKey		  	string
	licenseKey    	string
	accountId		int
	region        	region.Name
	eventsEnabled 	bool
	logsEnabled		bool
}

func newLabsIntegration(
	name, id string,
	app *newrelic.Application,
	integration *nriSdk.Integration,
	logger *logrus.Logger,
	runAsService bool,
	dryRun bool,
	labsIntegrationOpts []LabsIntegrationOpt,
) (*LabsIntegration, error) {
	li := &LabsIntegration{
		Name: name,
		Id: id,
		BuildInfo: build.GetBuildInfo(),
		App: app,
		Integration: integration,
		Logger: logger,
		Interval: DEFAULT_INTERVAL,
		RunAsService: runAsService,
		DryRun: dryRun,
		pipelines: []pipeline.Pipeline{},
	}

	for _, opt := range labsIntegrationOpts {
		err := opt(li)
		if err != nil {
			return nil, err
		}
	}

	return li, nil
}

func (i *LabsIntegration) AddPipeline(p pipeline.Pipeline) {
	i.pipelines = append(i.pipelines, p)
}

func (i *LabsIntegration) Run(ctx context.Context) error {
	// Run once
	if !i.RunAsService {
		errors := i.executeSync(ctx)
		if errors != nil {
			return errors
		}

		return nil
	}

	// Run as a service on a recurring interval
	var wg sync.WaitGroup

	for j := 0; j < len(i.pipelines); j++ {
		i.pipelines[j].Start(ctx, &wg)
	}

	wg.Add(1)
	go pollerWorker(ctx, i, &wg)

	wg.Wait()

	return nil
}

func showVersionAndExit(integrationName string) {
	buildInfo := build.GetBuildInfo()

	// Show the version
	fmt.Printf(
		"%s Version: %s, Platform: %s, GoVersion: %s, GitCommit: %s, BuildDate: %s\n",
		integrationName,
		buildInfo.Version,
		fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
		runtime.Version(),
		buildInfo.Commit,
		buildInfo.Date,
	)
	os.Exit(0)
}

func pollerWorker(
	ctx context.Context,
	i *LabsIntegration,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	// Poll on startup
	i.execute(ctx)

	// Setup ticker for subsequent polling
	ticker := time.NewTicker(time.Duration(i.Interval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return // returning not to leak the goroutine

		case <-ticker.C:
			log.Debugf("poll interval timer ticked; executing integration")
			i.execute(ctx)
		}
	}
}

func (i *LabsIntegration) execute(ctx context.Context) {
	log.Debugf("executing pipelines...")
	for j := 0; j < len(i.pipelines); j++ {
		i.pipelines[j].Execute(ctx)
	}
}

func (i *LabsIntegration) executeSync(ctx context.Context) error {
	for j := 0; j < len(i.pipelines); j++ {
		errs := i.pipelines[j].ExecuteSync(ctx)
		if len(errs) > 0 {
			return errors.Join(errs...)
		}
	}

	return nil
}

func (i *LabsIntegration) Shutdown(ctx context.Context) {
	log.Debugf("shutting down")

	if i.RunAsService {
		for j := 0; j < len(i.pipelines); j++ {
			p := i.pipelines[j]
			p.Shutdown(ctx)
		}
	}

	if i.eventsEnabled || i.logsEnabled {
		err := i.flushDataAndWait()
		if err != nil {
			i.Logger.Warnf("flush event queue to New Relic failed: %v", err)
		}
	}

	if i.App != nil {
		log.Debugf("shutting down APM agent")
		i.App.Shutdown(time.Second * 3)
	}
}

func WithLicenseKey() LabsIntegrationOpt {
	return func(li *LabsIntegration) error {
		licenseKey, err := getLicenseKey()
		if err != nil {
			return err
		}

		region, err := getNrRegion()
		if err != nil {
			return err
		}

		li.region = region
		li.licenseKey = licenseKey

		return nil
	}
}

func WithApiKey() LabsIntegrationOpt {
	return func(li *LabsIntegration) error {
		apiKey, err := getApiKey()
		if err != nil {
			return err
		}

		region, err := getNrRegion()
		if err != nil {
			return err
		}

		li.region = region
		li.apiKey = apiKey

		return nil
	}
}

func WithAccountId() LabsIntegrationOpt {
	return func(li *LabsIntegration) error {
		accountId, err := getAccountId()
		if err != nil {
			return err
		}

		li.accountId = accountId

		return nil
	}
}

func WithClient() LabsIntegrationOpt {
	return func(li *LabsIntegration) error {
		return setupClient(li)
	}
}

func WithEvents(ctx context.Context) LabsIntegrationOpt {
	return func(li *LabsIntegration) error {
		return setupEvents(ctx, li)
	}
}

func WithLogs(ctx context.Context) LabsIntegrationOpt {
	return func(li *LabsIntegration) error {
		return setupLogs(ctx, li)
	}
}

func WithInterval(defaultInterval int) LabsIntegrationOpt {
	return func(li *LabsIntegration) error {
		interval := viper.GetInt("interval")
		if interval == 0 {
			interval = defaultInterval
		}

		li.Interval = time.Duration(interval)

		return nil
	}
}

func (i *LabsIntegration) GetRegion() region.Name {
	return i.region
}

func (i *LabsIntegration) GetLicenseKey() string {
	return i.licenseKey
}

func (i *LabsIntegration) GetApiKey() string {
	return i.apiKey
}

func (i *LabsIntegration) AddEvent(
	ctx context.Context,
	eventType string,
	attributes map[string]string,
) error {
	if !i.eventsEnabled {
		return fmt.Errorf("failed to add event, events not enabled")
	}

	evt := make(map[string]string)
	evt["eventType"] = eventType

	for k, v := range attributes {
		evt[k] = v
	}

	if err := i.NrClient.Events.EnqueueEvent(ctx, evt); err != nil {
		return fmt.Errorf("failed to add event: %w", err)
	}

	return nil
}

func (i *LabsIntegration) flushDataAndWait() error {
	if i.eventsEnabled {
		err := i.NrClient.Events.Flush()
		if err != nil {
			return err
		}
	}

	if i.logsEnabled {
		err := i.NrClient.Logs.Flush()
		if err != nil {
			return err
		}
	}

	<-time.After(3 * time.Second)

	return nil
}


func configLicenseKey(licenseKey string) nrClient.ConfigOption {
	return func(cfg *config.Config) error {
		cfg.LicenseKey = licenseKey
		return nil
	}
}

func loadConfig() error {
	envPrefix := viper.GetString("env_prefix")

	viper.AutomaticEnv()
	if envPrefix != "" {
		viper.SetEnvPrefix(envPrefix)
	}

	// @TODO: When Viper officially releases this function, use it to add
	// an env replace to convert to screaming snake case.
	// The reason we can't do this now is because we use the global viper
	// instance. We don't create our own Viper instance using NewWithOptions()
	// so we can't specify options. The reason we want to do this is because our
	// config file naming standard uses camelcase keys but environment variables
	// should use screaming snake case. And the reason we can't use the existing
	// SetEnvKeyReplacer is that that can only be used to replace one character
	// with another and not do more complex replacements.
	// viper.SetOptions(WithEnvReplacer(...))
	// For now, at least replace . with _ so that nested key lookups can be
	// done using shell-safe environment variable ids.
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	configPath := viper.GetString("config_path")
	if configPath == "" {
		return NewConfigWithPaths()
	}

	return NewConfigWithFile(configPath)
}

func setupLogging(logger *logrus.Logger) error {
	verbose := viper.GetBool("verbose")

	if viper.IsSet("log.fileName") {
		file, err := os.OpenFile(
			viper.GetString("log.fileName"),
			os.O_CREATE|os.O_WRONLY|os.O_APPEND,
			0666,
		)
		if err != nil {
			log.Warnf("failed to log to file, using default stderr: %s", err)
		} else {
			logger.Out = file
		}
	}

	if verbose {
		logger.SetLevel(logrus.DebugLevel)
		return nil
	}

	logLevel := viper.GetString("log.level")
	if logLevel != "" {
		level, err := logrus.ParseLevel(logLevel)
		if err != nil {
			log.Warnf("failed to parse log level, default will be used: %s", err)
		} else {
			logger.SetLevel(level)
		}
	}

	return nil
}

func setupClient(li *LabsIntegration) error {
	opts := []nrClient.ConfigOption{}

	opts = append(opts, nrClient.ConfigLogger(
		logging.NewLogrusLogger(logging.ConfigLoggerInstance(li.Logger)),
	))

	if li.licenseKey != "" {
		opts = append(opts, configLicenseKey(li.licenseKey))
	}

	if li.apiKey != "" {
		opts = append(opts, nrClient.ConfigPersonalAPIKey(li.apiKey))
	}

	if li.region != "" {
		opts = append(opts, nrClient.ConfigRegion(li.region.String()))
	}

	// Initialize the New Relic Go Client
	client, err := nrClient.New(opts...)
	if err != nil {
		return fmt.Errorf("error creating New Relic client: %v", err)
	}

	li.NrClient = client

	return nil
}

func setupEvents(ctx context.Context, li *LabsIntegration) error {
	if li.NrClient == nil {
		err := setupClient(li)
		if err != nil {
			return err
		}
	}

	if li.accountId == 0 {
		accountId, err := getAccountId()
		if err != nil {
			return err
		}

		li.accountId = accountId
	}

	// Start batch mode
	if err := li.NrClient.Events.BatchMode(ctx, li.accountId); err != nil {
		return fmt.Errorf("error starting batch events mode: %w", err)
	}

	li.eventsEnabled = true

	return nil
}

func setupLogs(ctx context.Context, li *LabsIntegration) error {
	if li.NrClient == nil {
		err := setupClient(li)
		if err != nil {
			return err
		}
	}

	// We don't validate or get account ID here because it is not needed to
	// send logs. Even though it is required by the BatchMode() call, it is
	// not used in the implementation. So if we pass 0, it won't cause errors.

	// Start batch mode
	if err := li.NrClient.Logs.BatchMode(ctx, li.accountId); err != nil {
		return fmt.Errorf("error starting batch logs mode: %w", err)
	}

	li.logsEnabled = true

	return nil
}
