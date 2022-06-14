package archive

import (
	"io/ioutil"
	"log"
	"os"

	"gopkg.in/yaml.v3"
)

type RootYaml struct {
	Config       Config
	Archive_list map[string]ArchiveElement
}

type Config struct {
	Cloud_provider     string
	Aws_s3_bucket_name string
	Timestamp_column   string
}

type ArchiveElement struct {
	Db             string
	Db_name        string
	Schema_name    string
	Tables         []string
	No_of_days     int
	Cloud_provider string
}

func ParseYaml() (map[string]RootYaml, error) {
	configFilePath := os.Getenv("ARCHIVE_MANAGER_CONFIG_PATH")
	if configFilePath == "" {
		configFilePath = "config.yaml"
	}

	yfile, err := ioutil.ReadFile(configFilePath)

	if err != nil {
		log.Fatal(err)
	}

	data := make(map[string]RootYaml)
	err = yaml.Unmarshal(yfile, &data)

	if err != nil {
		log.Fatal(err)
	}

	return data, err
}
