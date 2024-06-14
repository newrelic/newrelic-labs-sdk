package integration

import (
	"fmt"
	"os"
	"strconv"

	"github.com/newrelic/newrelic-client-go/pkg/region"
	"github.com/spf13/viper"
)

func getLicenseKey() (string, error) {
	licenseKey := viper.GetString("licenseKey")
	if licenseKey == "" {
		licenseKey = os.Getenv("NEW_RELIC_LICENSE_KEY")
		if licenseKey == "" {
			return "", fmt.Errorf("missing New Relic license key")
		}
	}
	return licenseKey, nil
}

func getApiKey() (string, error) {
	apiKey := viper.GetString("apiKey")
	if apiKey == "" {
		apiKey = os.Getenv("NEW_RELIC_API_KEY")
		if apiKey == "" {
			return "", fmt.Errorf("missing New Relic API key")
		}
	}
	return apiKey, nil
}

func getNrRegion() (region.Name, error) {
	nrRegion := viper.GetString("region")
	if nrRegion == "" {
		nrRegion = os.Getenv("NEW_RELIC_REGION")
		if nrRegion == "" {
			nrRegion = string(region.Default)
		}
	}

	r, err := region.Parse(nrRegion)
	if err != nil {
		return "", err
	}

	return r, nil
}

func getAccountId() (int, error) {
	accountId := viper.GetInt("accountId")
	if accountId == 0 {
		eventsAccountId := os.Getenv("NEW_RELIC_ACCOUNT_ID")
		if eventsAccountId == "" {
			return 0, fmt.Errorf("missing New Relic account ID")
		}

		var err error

		accountId, err = strconv.Atoi(eventsAccountId)
		if err != nil {
			return 0, fmt.Errorf("invalid New Relic account ID %s", eventsAccountId)
		}
	}
	return accountId, nil
}
