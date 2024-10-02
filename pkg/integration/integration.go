package integration

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/newrelic/go-agent/v3/newrelic"
	nriSdk "github.com/newrelic/infra-integrations-sdk/v4/integration"
	nrClient "github.com/newrelic/newrelic-client-go/v2/newrelic"
	"github.com/newrelic/newrelic-client-go/v2/pkg/nrdb"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/build"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/log"

	"github.com/newrelic/newrelic-client-go/v2/pkg/config"
	"github.com/newrelic/newrelic-client-go/v2/pkg/logging"
	"github.com/newrelic/newrelic-client-go/v2/pkg/region"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	DEFAULT_INTERVAL = 60
	LAST_UPDATE_EVENT_TYPE = "IntegrationLastUpdate"
	LAST_UPDATE_INTEGRATION_ID_ATTR_NAME = "integrationId"
	LAST_UPDATE_COMPONENT_ID_ATTR_NAME = "componentId"
)

type (
	LabsIntegrationOpt func(li *LabsIntegration) error
	LastUpdateFunc func(lastUpdate time.Time) error
	ExecuteSyncFunc func(ctx context.Context, li *LabsIntegration) error
)

type Identifiable interface {
	GetId() string
}

type Component interface {
	Identifiable
	Start(ctx context.Context, wg *sync.WaitGroup) error
	Execute(ctx context.Context) error
	ExecuteSync(ctx context.Context) error
	Shutdown(ctx context.Context) error
}

type schedule struct {
	crontab 	string
	components	[]Component
}

type SimpleComponent struct {
	id			string
	i			*LabsIntegration
	executeFunc	ExecuteSyncFunc
}

func NewSimpleComponent(
	id string,
	i *LabsIntegration,
	executeFunc ExecuteSyncFunc,
) *SimpleComponent {
	return &SimpleComponent{ id, i, executeFunc }
}

func (s *SimpleComponent) GetId() string {
	return s.id
}

func (s *SimpleComponent) Start(ctx context.Context, wg *sync.WaitGroup) error {
	// no-op
	return nil
}

func (s *SimpleComponent) Execute(
	ctx context.Context,
) error {
	return s.ExecuteSync(ctx)
}

func (s *SimpleComponent) ExecuteSync(
	ctx context.Context,
) error {
	return s.executeFunc(ctx, s.i)
}

func (t *SimpleComponent) Shutdown(ctx context.Context) error {
	// no-op
	return nil
}

// @TODO: connectors Request() methods should take a context
// @TODO: support custom tags via config
// @TODO: hot reload of config

type LabsIntegration struct {
	Name					string
	Id						string
	BuildInfo				build.BuildInfo
	App           			*newrelic.Application
	Integration   			*nriSdk.Integration
	Logger        			*logrus.Logger
	Interval	  			time.Duration
	NrClient      			*nrClient.NewRelic
	RunAsService  			bool
	DryRun					bool
	components       		[]Component
	schedules				[]*schedule
	apiKey		  			string
	licenseKey    			string
	accountId				int
	region        			region.Name
	eventsEnabled 			bool
	logsEnabled				bool
	quitChan				chan struct{}
	runningLocker			sync.Mutex
	running					bool
	lastUpdateEnabled		bool
}

func newLabsIntegration(
	name, id string,
	app *newrelic.Application,
	integration *nriSdk.Integration,
	logger *logrus.Logger,
	runAsService bool,
	dryRun bool,
	labsIntegrationOpts ...LabsIntegrationOpt,
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
		components: []Component{},
		schedules: []*schedule{},
	}

	for _, opt := range labsIntegrationOpts {
		err := opt(li)
		if err != nil {
			return nil, err
		}
	}

	return li, nil
}

func (i *LabsIntegration) AddComponent(
	c Component,
) {
	i.components = append(i.components, c)
}

func (i *LabsIntegration) AddSchedule(
	crontab string,
	components []Component,
) {
	sched := &schedule{
		crontab: crontab,
		components: components,
	}

	i.schedules = append(i.schedules, sched)
}

func (i *LabsIntegration) Run(ctx context.Context) error {
	log.Debugf("executing integration")

	numComponents := len(i.components)
	numSchedules := len(i.schedules)

	if numComponents == 0 && numSchedules == 0 {
		log.Infof("nothing to do")
		return nil
	}

	// Run once
	if !i.RunAsService {
		i.setRunning()

		errors := i.executeSync(ctx)
		if errors != nil {
			return errors
		}

		return nil
	}

	// Run as a service on a recurring interval
	var wg sync.WaitGroup

	if numComponents > 0 {
		log.Debugf("starting %d components", numComponents)

		for j := 0; j < numComponents; j += 1 {
			c := i.components[j]

			log.Debugf("starting component %s", c.GetId())

			err := c.Start(ctx, &wg)
			if err != nil {
				return fmt.Errorf(
					"component %s failed to start: %v",
					c.GetId(),
					err,
				)
			}
		}
	}

	// Add any schedules
	if numSchedules > 0 {
		s, err := gocron.NewScheduler()
		if err != nil {
			return err
		}

		log.Debugf("registering %d schedules", numSchedules)

		for _, sched := range i.schedules {
			numScheduledComponents := len(sched.components)

			if numScheduledComponents == 0 {
				continue
			}

			log.Debugf(
				"starting %d scheduled components",
				numScheduledComponents,
			)

			for _, c := range sched.components {
				log.Debugf("starting scheduled component %s", c.GetId())

				err := c.Start(ctx, &wg)
				if err != nil {
					return fmt.Errorf(
						"scheduled component %s failed to start: %v",
						c.GetId(),
						err,
					)
				}
			}

			job, err := s.NewJob(
				gocron.CronJob(sched.crontab, false),
				gocron.NewTask(
					func (s *schedule) {
						log.Debugf("scheduled task called")
						i.executeComponents(ctx, s.components)
					},
					sched,
				),
			)
			if err != nil {
				return err
			}

			log.Debugf(
				"registered job %s on schedule %s",
				job.ID().String(),
				sched.crontab,
			)
		}

		log.Debugf("starting scheduler")
		s.Start()
		defer func () {
			err := s.Shutdown()
			if err != nil {
				log.Warnf("scheduler failed to shutdown properly: %v", err)
			}
		}()
	}

	if numComponents > 0 {
		i.quitChan = make(chan struct{})

		wg.Add(1)

		log.Debugf("starting executor")
		go executeWorker(ctx, i, &wg)
	}

	i.installInterruptHandler(ctx)

	i.setRunning()

	wg.Wait()

	return nil
}

func (i *LabsIntegration) shouldShutdown() bool {
	// cheap check first to avoid locking if we can
	if !i.running {
		return false
	}

	i.runningLocker.Lock()
	defer i.runningLocker.Unlock()

	// recheck to see if it shutdown while we were waiting for the lock
	if !i.running {
		return false
	}

	i.running = false

	return true
}

func (i *LabsIntegration) setRunning() {
	i.runningLocker.Lock()
	defer i.runningLocker.Unlock()
	i.running = true
}

func (i *LabsIntegration) installInterruptHandler(ctx context.Context) {
	// see https://pkg.go.dev/os/signal#example-Notify

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	go func() {
		for {
			select {
			case <-c:
				i.Shutdown(ctx)
				return

			case <-ctx.Done():
				return
			}
		}
	}()
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

func executeWorker(
	ctx context.Context,
	i *LabsIntegration,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	// Execute on startup
	i.executeComponents(ctx, i.components)

	// Setup ticker to execute on an interval
	ticker := time.NewTicker(time.Duration(i.Interval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <- i.quitChan:
			log.Debugf("shutting down executor")
			return

		case <-ticker.C:
			// Make sure that the timer didn't tick in between when the close
			// flag was set on the quitChan and when the receive above actually
			// happens.

			select {
			case _, ok := <- i.quitChan:
				if !ok {
					// channel close flag was set, integration shutting down,
					// don't try to execute components
					return
				}
			default:
				// reading from quitChan would block so it's close flag is not
				// set and we can proceed to execute components
			}

			log.Debugf("poll interval timer ticked; executing components")
			i.executeComponents(ctx, i.components)

		case <-ctx.Done():
			return // returning not to leak the goroutine
		}
	}
}

func (i *LabsIntegration) executeComponents(
	ctx context.Context,
	components []Component,
) {
	log.Debugf("executing %d components...", len(components))

	for j := 0; j < len(components); j += 1 {
		c := components[j]

		log.Debugf("executing component %s", c.GetId())

		err := c.Execute(ctx)
		if err != nil {
			log.Errorf(
				"execute component %s failed: %v",
				c.GetId(),
				err,
			)
		}
	}
}

func (i *LabsIntegration) executeSync(ctx context.Context) error {
	log.Debugf(
		"executing %d components synchronously...",
		len(i.components),
	)

	for j := 0; j < len(i.components); j += 1 {
		c := i.components[j]

		log.Debugf("executing component %s synchronously", c.GetId())

		err := c.ExecuteSync(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

func (i *LabsIntegration) Shutdown(ctx context.Context) {
	log.Debugf("shutting down integration")

	if !i.shouldShutdown() {
		log.Debugf("integration already shutdown")
		return
	}

	if i.RunAsService {
		numComponents := len(i.components)
		numSchedules := len(i.schedules)

		if numComponents > 0 {
			log.Debugf("shutting down %d components", numComponents)

			// shutdown all the components
			for j := 0; j < numComponents; j += 1 {
				c := i.components[j]

				log.Debugf("shutting down component %s", c.GetId())

				err := c.Shutdown(ctx)
				if err != nil {
					log.Warnf(
						"component %s failed to shutdown properly: %v",
						c.GetId(),
						err,
					)
				}
			}
		}

		if numSchedules > 0 {
			log.Debugf("shutting down %d schedules", numSchedules)

			// shutdown all scheduled components
			for _, sched := range i.schedules {
				numScheduledComponents := len(sched.components)

				if numScheduledComponents == 0 {
					continue
				}

				log.Debugf(
					"shutting down %d scheduled components",
					numScheduledComponents,
				)

				for _, c := range sched.components {
					log.Debugf(
						"shutting down scheduled component %s",
						c.GetId(),
					)

					err := c.Shutdown(ctx)
					if err != nil {
						log.Warnf(
							"scheduled component %s failed to shutdown properly: %v",
							c.GetId(),
							err,
						)
					}
				}
			}
		}
	}

	if i.eventsEnabled || i.logsEnabled {
		err := i.flushDataAndWait()
		if err != nil {
			i.Logger.Warnf("flush event queue to New Relic failed: %v", err)
		}
	}

	if i.App != nil {
		defer func() {
			log.Debugf("shutting down APM agent")
			// shutting down APM hoses the logger because the nrlogrus provider
			// will still try to send via the agent and this will cause error
			// messages on the console. So we reset back to the text formatter.
			// This does mean that in context logging will not grab the last few
			// log messages but oh well.
			log.RootLogger.SetFormatter(&logrus.TextFormatter{})
			i.App.Shutdown(time.Second * 3)
		}()
	}

	// @todo: waiting to shutdown the executor until this late allows for the
	// possibility of the timer firing and attempting to execute components
	// after they've been shutdown. But the executor goroutine also keeps the
	// main thread open while we shutdown gracefully. Need to find a way to keep
	// the main thread open but also stop the executor timer before we shutdown.
	if i.RunAsService && len(i.components) > 0 {
		close(i.quitChan)
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

func WithLastUpdate() LabsIntegrationOpt {
	return func(li *LabsIntegration) error {
		return setupLastUpdate(li)
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

func (i *LabsIntegration) GetAccountId() int {
	return i.accountId
}

func (i *LabsIntegration) AddEvent(
	ctx context.Context,
	eventType string,
	attributes map[string]interface{},
) error {
	if !i.eventsEnabled {
		return fmt.Errorf("failed to add event, events not enabled")
	}

	evt := make(map[string]interface{})
	evt["eventType"] = eventType

	for k, v := range attributes {
		evt[k] = v
	}

	if err := i.NrClient.Events.EnqueueEvent(ctx, evt); err != nil {
		return fmt.Errorf("failed to add event: %w", err)
	}

	return nil
}

func (i *LabsIntegration) AddEventImmediate(
	ctx context.Context,
	eventType string,
	attributes map[string]interface{},
) error {
	evt := make(map[string]interface{})
	evt["eventType"] = eventType

	for k, v := range attributes {
		evt[k] = v
	}

	if err := i.NrClient.Events.CreateEvent(i.accountId, evt); err != nil {
		return fmt.Errorf("failed to add event: %w", err)
	}

	return nil
}

func (i *LabsIntegration) GetLastUpdateTimestamp(
	ctx context.Context,
	componentId string,
) (*time.Time, error) {
	if !i.lastUpdateEnabled {
		return nil, fmt.Errorf("last update not enabled")
	}

	log.Debugf(
		"querying for latest timestamp for integration %s and component %s",
		i.Id,
		componentId,
	)

	result, err := i.NrClient.Nrdb.QueryWithAdditionalOptionsWithContext(
		ctx,
		i.accountId,
		nrdb.NRQL(fmt.Sprintf(
			"SELECT latest(timestamp) FROM %s WHERE %s = '%s' AND %s = '%s' SINCE 1 MONTH AGO",
			LAST_UPDATE_EVENT_TYPE,
			LAST_UPDATE_INTEGRATION_ID_ATTR_NAME,
			i.Id,
			LAST_UPDATE_COMPONENT_ID_ATTR_NAME,
			componentId,
		)),
		5,
		false,
	)
	if err != nil {
	  	return nil, fmt.Errorf("query for last update failed: %w", err)
	}

	if result == nil || len(result.Results) == 0 {
		log.Warnf(
			"no results found searching for last update timestamp, returning zero time",
		)

		return &time.Time{}, nil
	}

	row := result.Results[0]
	val, ok := row["latest.timestamp"]
	if !ok {
		// if a result is found, this should never happen
		return nil, fmt.Errorf("no timestamp attribute found in result")
	}

	if val == nil {
		return &time.Time{}, nil
	}

	latestTimestamp, ok := val.(float64)
	if !ok {
		// same
		return nil, fmt.Errorf(
			"timestamp attribute found in result is not a float",
		)
	}

	log.Debugf("found latest timestamp %f", latestTimestamp)

	t := time.UnixMilli(int64(latestTimestamp))

	return &t, nil
}

func (i *LabsIntegration) AddLastUpdateTimestamp(
	ctx context.Context,
	componentId string,
) error {
	if !i.lastUpdateEnabled {
		return fmt.Errorf("last update not enabled")
	}

	log.Debugf(
		"adding latest timestamp for integration %s and component %s",
		i.Id,
		componentId,
	)

	if i.DryRun {
		return nil
	}

	err :=  i.AddEventImmediate(
		ctx,
		LAST_UPDATE_EVENT_TYPE,
		map[string]interface{} {
			LAST_UPDATE_INTEGRATION_ID_ATTR_NAME: i.Id,
			LAST_UPDATE_COMPONENT_ID_ATTR_NAME: componentId,
		},
	)
	if err != nil {
		return fmt.Errorf("add last update failed: %w", err)
	}

	log.Debugf("added latest timestamp")

	return nil
}

func (i *LabsIntegration) UseLastUpdate(
	ctx context.Context,
	lastUpdateFunc LastUpdateFunc,
	componentId string,
) error {
	lastUpdate, err := i.GetLastUpdateTimestamp(ctx, componentId)
	if err != nil {
		return err
	}

	err = lastUpdateFunc(*lastUpdate)
	if err != nil {
		return err
	}

	err = i.AddLastUpdateTimestamp(ctx, componentId)
	if err != nil {
		return err
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

func setupLastUpdate(li *LabsIntegration) error {
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

	li.lastUpdateEnabled = true

	return nil
}
