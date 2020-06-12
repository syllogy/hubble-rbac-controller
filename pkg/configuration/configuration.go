package configuration

import (
	"fmt"
	"os"
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
	} else {
		var message string
		for _,name := range e.Missing {
			message += fmt.Sprintf("env variable with name %s was not found, ", name)
		}
		return fmt.Errorf(message)
	}
}

type Configuration struct {
	GoogleCredentials string
	RedshiftHostTemplate string
	RedshiftUsername string
	RedshiftPassword string
	RedshiftMasterDatabase string
	AwsAccountId string
	Region string
	GoogleAdminPrincipalEmail string
}

func loadVariable(name string, errorCollector *ErrorCollector) string {
	if value, ok := os.LookupEnv(name); ok {
		return value
	} else {
		errorCollector.Register(name)
		return ""
	}
}

func LoadConfiguration() (Configuration, error) {

	errorCollector := &ErrorCollector{}
	result := Configuration{
		GoogleCredentials:loadVariable("GOOGLE_CREDENTIALS_FILE_PATH", errorCollector),
		RedshiftHostTemplate: "%s.cbhx6wm2xwwx.eu-west-1.redshift.amazonaws.com",
		RedshiftUsername:loadVariable("REDSHIFT_USERNAME", errorCollector),
		RedshiftPassword:loadVariable("REDSHIFT_PASSWORD", errorCollector),
		RedshiftMasterDatabase:"prod",
		AwsAccountId:loadVariable("AWS_ACCOUNT_ID", errorCollector),
		GoogleAdminPrincipalEmail:loadVariable("GOOGLE_ADMIN_PRINCIPAL_EMAIL", errorCollector),
		Region: "eu-west-1",
	}

	return result, errorCollector.Error()
}
