package archive

type AchManConfig struct {
	CloudProvider   string
	AwsS3BucketName string
	TimestampColumn string
}

var ConfigStore AchManConfig