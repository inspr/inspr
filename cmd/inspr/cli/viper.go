package cli

import (
	"os"
	"path/filepath"

	"github.com/spf13/viper"
)

const (
	configCurrentScope = "currentScope"
	configServerIP     = "serverIP"
)

var defaultValues map[string]string = map[string]string{
	configCurrentScope: "",
	configServerIP:     "127.0.0.1",
}

// initConfig - sets defaults values and where is the file in which new values can be read
func initViperConfig() {
	// specifies the path in which the config file present
	viper.AddConfigPath("$HOME/.inspr/")
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")

	for k, v := range defaultValues {
		viper.SetDefault(k, v)
	}
}

// createConfig - creates the folder and or file of the inspr's viper config
// if they already a file the createConfig will truncate it before writing
func createViperConfig() error {

	homeDir, err := os.UserHomeDir()
	if err != nil {
		return err
	}

	// folder path
	insprFolderDir := filepath.Join(homeDir, ".inspr")

	// creates folder
	if _, err := os.Stat(insprFolderDir); os.IsNotExist(err) {
		if err := os.Mkdir(insprFolderDir, 0666); err != nil { // perm 0666
			return err
		}
	}

	// file path
	fileDir := filepath.Join(insprFolderDir, "config")

	// creates config file
	err = viper.WriteConfigAs(fileDir)
	if err != nil {
		return err
	}
	return nil
}

// readConfig - reads the inspr's viper config, in case it didn't
// found any, it creates one with the defaults values
func readViperConfig() error {
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			if createErr := createViperConfig(); createErr != nil {
				err = createErr
			} else {
				err = nil
			}
		} else {
			return err
		}
		return err
	}
	return nil
}
