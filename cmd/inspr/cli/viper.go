package cli

import (
	"io/ioutil"
	"os"

	"github.com/spf13/viper"
	"gopkg.in/yaml.v2"
)

type configuration struct {
	port string
	mock string
}

var conf *configuration

var defaultValues map[string]string = map[string]string{
	"port": "8080",
	"mock": "default_mock",
}

// sets defaults values and where is the file in which new values can be read
func initViper() {
	// specifies the path in which the config file present
	viper.SetConfigType("yaml")
	viper.AddConfigPath("$HOME")
	viper.SetConfigName(".inspr-env")

	for k, v := range defaultValues {
		viper.SetDefault(k, v)
	}

}

func createConfig() error {
	homeDir := os.Getenv("$HOME")

	bytes, err := yaml.Marshal(defaultValues)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(homeDir+".inspr-env.yaml", bytes, 0777)
	if err != nil {
		return err
	}

	return nil
}
