package configuration

import (
	"fmt"
	"os"
	"strconv"
)

type ErrorCollector struct {
	Missing []string
}

func (e *ErrorCollector) Register(name string) {
	e.Missing = append(e.Missing, name)
}

func (e *ErrorCollector) Error() error {
	if len(e.Missing) == 0 {
		return nil
	}
	var message string
	for _, name := range e.Missing {
		message += fmt.Sprintf("env variable with name %s was not found, ", name)
	}
	return fmt.Errorf(message)
}

type Configuration struct {
	GoogleCredentials         string
	RedshiftHostTemplate      string
	RedshiftUsername          string
	RedshiftPassword          string
	RedshiftMasterDatabase    string
	AwsAccountId              string
	Region                    string
	GoogleAdminPrincipalEmail string
	DryRun                    bool
}

func loadVariable(name string, errorCollector *ErrorCollector) string {
	value, ok := os.LookupEnv(name)
	if !ok {
		errorCollector.Register(name)
		return ""
	}
	return value
}

func loadBool(name string, errorCollector *ErrorCollector) bool {
	value, ok := os.LookupEnv(name)
	if !ok {
		errorCollector.Register(name)
		return false
	}
	result, err := strconv.ParseBool(value)
	if err != nil {
		errorCollector.Register(name)
		return false
	}
	return result
}

func LoadConfiguration() (Configuration, error) {

	errorCollector := &ErrorCollector{}

	result := Configuration{
		GoogleCredentials:         loadVariable("GOOGLE_CREDENTIALS_FILE_PATH", errorCollector),
		RedshiftHostTemplate:      "%s.cbhx6wm2xwwx.eu-west-1.redshift.amazonaws.com",
		RedshiftUsername:          loadVariable("REDSHIFT_USERNAME", errorCollector),
		RedshiftPassword:          loadVariable("REDSHIFT_PASSWORD", errorCollector),
		RedshiftMasterDatabase:    "prod",
		AwsAccountId:              loadVariable("AWS_ACCOUNT_ID", errorCollector),
		GoogleAdminPrincipalEmail: loadVariable("GOOGLE_ADMIN_PRINCIPAL_EMAIL", errorCollector),
		Region:                    "eu-west-1",
		DryRun:                    loadBool("DRYRUN", errorCollector),
	}

	return result, errorCollector.Error()
}
