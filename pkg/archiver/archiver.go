package archiver

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"slices"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/getsentry/sentry-go"
	r "github.com/redis/go-redis/v9"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/coreos/go-systemd/daemon"
	"github.com/samber/lo"

	"github.com/xplore-run/archive-manager/pkg/infra/mongodb"
	"github.com/xplore-run/archive-manager/pkg/infra/redis"
	"github.com/xplore-run/archive-manager/pkg/lib/gcp"
	"github.com/xplore-run/archive-manager/pkg/lib/lock"
	logger "github.com/xplore-run/go-logger/pkg"
)

// Archiver fixed variables | Non env variables
var (
	ctx         = context.Background()
	mongoClient *mongo.Client
	redisClient *redis.Client
	logr        *logger.CustomLogger
	redisLock   *lock.RedisLockService

	shutdownMutex       sync.Mutex
	docsBufferMutex     sync.Mutex
	Shutdown            = false
	lastFlushTime       = time.Now()
	redisLockIdentifier = fmt.Sprintf("%v-%d", time.Now().UnixNano(), os.Getpid())

	lastID = primitive.NilObjectID
)

// Archiver dynamic variables | Env variables
var (
	ArchiverName = os.Getenv("ARCHIVER_NAME")

	mongoCollectionName = os.Getenv("MONGO_COLLECTION_NAME")

	redisLastIDKey    = os.Getenv("REDIS_LAST_ID_KEY")
	redisLockKey      = os.Getenv("REDIS_LOCK_KEY")
	redisProcessedKey = os.Getenv("REDIS_PROCESSED_KEY")
	maxID             = os.Getenv("MAX_ID")
	upto              = os.Getenv("UPTO") // 1h, 1d, 1w, 1m

	partitionPrefix = os.Getenv("PARTITION_PREFIX")
	writerType      = os.Getenv("WRITER_COLLECTION_TYPE")
)

var uptoDuration time.Duration

func init() {
	var err error
	uptoDuration, err = time.ParseDuration(upto)
	if err != nil {
		log.Fatalf("Error parsing upto duration: %v", err)
	}
}

func Type() WrtierType {
	if writerType == "hourly_partition" {
		return WrtierTypeHourlyPartition
	}
	return WrtierTypeNoPartition
}

func MongoFetchLimit() int64 {
	return 10 * 1000 // 10k
}

// FetchInterval
func MongoFetchInterval() time.Duration {
	return 10 * time.Second
}

func BufferLimit() int64 {
	return 400 * 1000 // 400k
}

func BufferFlushDuration() time.Duration {
	return 60 * time.Second
}

// REDIS_KEY_LAST_CLICK
func GetLastIDRedisKey() string {
	return redisLastIDKey
}

// REDIS_LOCK_KEY
func GetRedisLockKey() string {
	return redisLockKey
}

// MAX_ID
func GetMaxID() string {
	return maxID
}

// Mongodb collection name
func GetMongoCollectionName() string {
	return mongoCollectionName
}

// GetProcessedCollectionRedisKey
func GetProcessedCollectionRedisKey() string {
	return redisProcessedKey
}

// PartitionPrefix
func PartitionPrefix() string {
	return partitionPrefix
}

type WrtierType = string

const (
	WrtierTypeHourlyPartition WrtierType = "hourly_partition"
	WrtierTypeNoPartition     WrtierType = "no_partition"
)

type Writer interface {
	WriteDocumentsToCSV(appdir string, docs []interface{}) error
	ExtraFilter() (bson.M, error)
}

type Archiver struct {
	Name string
	Docs []interface{}
	Writer
}

// StartArchiver is the main function to start the streamer
func (st *Archiver) StartArchiver() {
	// recover and log it
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("[StartArchiver] panic: %v", r)
			os.Exit(1)
		}
	}()

	// Initialize Logger
	logr = InitLogger(os.Getenv("APP_DIR"), fmt.Sprintf("%s logger.Log", st.Name))
	logr.Info(ctx, fmt.Sprintf("[%s] Starting streamer", st.Name))

	// Initialize Mongo Client
	mongoClient, err := mongodb.NewMongoClient(os.Getenv("DEFAULT_MONGODB_URL"))
	if err != nil {
		log.Fatalf("[mongodb] error- %s", err)
	}
	defer mongoClient.Close(ctx) // close mongo client

	// Initialize Redis Client
	redisClient, err := redis.NewRedisClient()
	if err != nil {
		log.Fatalf("[redis] error- %s", err)
	}
	defer redisClient.Close() // close redis client

	logr.Info(ctx, fmt.Sprintf("[%s] Initializing redis lock", st.Name))
	redisLock = lock.NewRedisLockService(redisClient.GetClient())
	logr.Info(ctx, fmt.Sprintf("[%s] Redis lock initialized", st.Name))

	// Add shutdown hooks
	addShutdownHooks()

	// Notify systemd that the service is ready
	daemon.SdNotify(false, "READY=1")

	// Start the processing based on the writer type
	if Type() == WrtierTypeHourlyPartition {
		err = st.processHourlyPartitionedCollections(ctx, mongoClient.GetDefaultDB(os.Getenv("DEFAULT_MOGO_DB")), redisClient, logr)
	} else {
		err = st.processNoPartitionCollections(ctx, mongoClient.GetDefaultDB(os.Getenv("DEFAULT_MOGO_DB")), redisClient, logr)
	}

	if err != nil {
		Log(ctx, logr, "error", fmt.Sprintf("[StartArchiver] error: %v", err))
	}
}

func addShutdownHooks() {
	c := make(chan os.Signal, 1)
	// handle debug stop signal
	signal.Notify(c, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		for sig := range c {
			fmt.Printf("%v Signal was received at %s. Shutting down the consumer\n", sig, time.Now().Format(time.RFC3339))
			shutdownMutex.Lock()
			Shutdown = true
			shutdownMutex.Unlock()
			redisLock.ReleaseLock(ctx, GetRedisLockKey(), redisLockIdentifier)
			fmt.Printf("Lock released.. lockKey %s, lockKeyIdentifier %s \n", GetRedisLockKey(), redisLockIdentifier)
		}
	}()
}

func InitLogger(logFolder, logFileName string) *logger.CustomLogger {
	loggerConfig := logger.LoggerConfig{
		TimeFormat: "02-01-2006 15:04:05",
		SinkType:   logger.FILE,
		FileSinkConfig: &logger.LoggerFileSinkConfig{
			FilePath:   fmt.Sprintf("%s/logs/%s", logFolder, logFileName),
			MaxSize:    1,
			MaxBackups: 2,
			MaxAge:     1,
			Compress:   true,
		},
		BatchSize:    10,
		FlushTimeout: 5 * time.Second,
	}

	customLogger, err := logger.NewCustomLogger(loggerConfig)
	if err != nil {
		log.Fatalf("Failed to create custom logger: %s", err.Error())
	}
	return customLogger
}

func Log(ctx context.Context, logr *logger.CustomLogger, level string, msg string) {
	log.Println(msg)

	switch level {
	case "info":
		logr.Info(ctx, msg)
	case "error":
		logr.Error(ctx, msg)
	case "warn":
		logr.Warn(ctx, msg)
	case "debug":
		logr.Debug(ctx, msg)
	}
}

/**
*********************** Process No Partition Collections ***************************
**/
func (st *Archiver) processNoPartitionCollections(ctx context.Context, mongoDB *mongo.Database, redisClient *redis.Client, logr *logger.CustomLogger) error {
	var threshold = 0
	if len(GetMaxID()) == 24 {
		Log(ctx, logr, "info", fmt.Sprintf("[archive-manager-archive-manager-pnpc] Archiving docs upto to MAX_ID: %s\n", GetMaxID()))
	}
	if uptoDuration > 0 {
		Log(ctx, logr, "info", fmt.Sprintf("[archive-manager-pnpc] Archiving docs for upto %f hours \n", uptoDuration.Hours()))
	}
	fmt.Printf("[archive-manager-pnpc] lockKey %s,  LAST_%s_REDIS_KEY %s \n", GetRedisLockKey(), st.Name, GetLastIDRedisKey())

	// Acquire lock
	lockAcquired, err := redisLock.AcquireLockWithRetry(ctx, lock.AcquireLockWithRetryArgs{
		LockName:      GetRedisLockKey(),
		Identifier:    redisLockIdentifier,
		Timeout:       0, // forever
		RetryInterval: 5 * time.Second,
		Retry:         2,
	})

	// Check if lock is acquired
	if err != nil || lockAcquired == "" {
		Log(ctx, logr, "error", fmt.Sprintf("[archive-manager-pnpc] Failed to acquire lock: %v", err))
		return fmt.Errorf("[archive-manager-pnpc] Failed to acquire lock: %v", err)
	}

	// Check if lock is acquired by another process
	if lockAcquired != redisLockIdentifier {
		Log(ctx, logr, "error", "[archive-manager-pnpc] Lock acquired by another process")
		return fmt.Errorf("[archive-manager-pnpc] Lock acquired by another process")
	}
	Log(ctx, logr, "info", fmt.Sprintf("[archive-manager-pnpc] Lock acquired successfully.. lockKey %s, lockKeyIdentifier %s \n", GetRedisLockKey(), redisLockIdentifier))

	// Gained control of the lock
	for {
		shutdownMutex.Lock()
		if Shutdown {
			shutdownMutex.Unlock()
			fmt.Printf("[archive-manager-pnpc] shutdown signal received- flushing buffer\n")
			if err := st.FlushBuffer(); err != nil {
				Log(ctx, logr, "error", fmt.Sprintf("[archive-manager-pnpc][archiveBuffer] error: %v\n", err))
			}
			return nil
		}
		shutdownMutex.Unlock()

		docs, err := st.fetchDocumentsFromMongo(ctx, logr, mongoDB, redisClient, GetMongoCollectionName())
		if err != nil {
			Log(ctx, logr, "error", fmt.Sprintf("[archive-manager-pnpc] Failed to fetch docs: %v", err))
			time.Sleep(MongoFetchInterval())
			continue
		}

		docsBufferMutex.Lock()
		st.Docs = append(st.Docs, docs...)
		docsBufferMutex.Unlock()
		threshold += len(docs)

		if threshold >= int(BufferLimit()) || (time.Now().Unix()-lastFlushTime.Unix()) > int64(BufferFlushDuration().Seconds()) {
			if err := st.FlushBuffer(); err != nil {
				fmt.Printf("[archive-manager-pnpc][archiveBuffer] error: %v\n", err)
				syscall.Kill(syscall.Getpid(), syscall.SIGINT)
				return err
			}
			threshold = 0
		}

		// Sleep for the fetch interval
		time.Sleep(MongoFetchInterval())
	}
}

/**
*********************** Process Hourly Partitioned Collections ***************************
**/

// Processes all collections partitioned by the current hour using Redis Sorted Set to track processed collections.
func (st *Archiver) processHourlyPartitionedCollections(ctx context.Context, mongoDB *mongo.Database, redisClient *redis.Client, logr *logger.CustomLogger) error {
	if len(GetMaxID()) == 24 {
		Log(ctx, logr, "info", fmt.Sprintf("[archive-manager-archive-manager-phpc] Archiving docs upto to MAX_ID: %s\n", GetMaxID()))
	}
	if uptoDuration > 0 {
		Log(ctx, logr, "info", fmt.Sprintf("[archive-manager-pnpc] Archiving docs for upto %f hours \n", uptoDuration.Hours()))
	}
	fmt.Printf("[archive-manager-phpc] lockKey %s,  LAST_%s_REDIS_KEY %s \n", GetRedisLockKey(), st.Name, GetLastIDRedisKey())

	// Acquire lock
	lockAcquired, err := redisLock.AcquireLockWithRetry(ctx, lock.AcquireLockWithRetryArgs{
		LockName:      GetRedisLockKey(),
		Identifier:    redisLockIdentifier,
		Timeout:       0, // forever
		RetryInterval: 5 * time.Second,
		Retry:         2,
	})

	// Check if lock is acquired
	if err != nil || lockAcquired == "" {
		Log(ctx, logr, "error", fmt.Sprintf("[archive-manager-phpc] Failed to acquire lock: %v", err))
		return fmt.Errorf("[archive-manager-phpc] Failed to acquire lock: %v", err)
	}

	// Check if lock is acquired by another process
	if lockAcquired != redisLockIdentifier {
		Log(ctx, logr, "error", "[archive-manager-phpc] Lock acquired by another process")
		return fmt.Errorf("[archive-manager-phpc] Lock acquired by another process")
	}
	Log(ctx, logr, "info", fmt.Sprintf("[archive-manager-phpc] Lock acquired successfully.. lockKey %s, lockKeyIdentifier %s \n",
		GetRedisLockKey(), redisLockIdentifier))

	collections, err := mongoDB.ListCollectionNames(ctx, bson.M{})
	if err != nil {
		Log(ctx, logr, "error", fmt.Sprintf("[archive-manager-phpc] Error fetching collections: %v", err))
		return err
	}

	collectionNames := lo.Filter(collections, func(col string, _ int) bool {
		return strings.HasPrefix(col, PartitionPrefix())
	})

	// Sort collections in ascending order based on the timestamp part of the collection name (YYYYMMDDHH)
	sort.Strings(collectionNames)
	processedCollectionsSetKey := GetProcessedCollectionRedisKey()

	// get all processed collections from redis
	processedCollections, err := redisClient.GetClient().ZRange(ctx, processedCollectionsSetKey, 0, -1).Result()
	if err != nil {
		Log(ctx, logr, "error", fmt.Sprintf("[archive-manager-phpc] Failed to get processed collections: %v", err))
		return err
	}

	// filter out the processed collections from the collectionNames
	collectionNames = lo.Filter(collectionNames, func(col string, _ int) bool {
		return !lo.Contains(processedCollections, col)
	})

	threshold := 0
	// process each collection one by one except the current hour one
	for _, collectionName := range collectionNames {
		// Collection names are in the format `clicks_YYYYMMDDHH`
		timestampStr := strings.TrimPrefix(collectionName, "clicks_")
		collectionTime, err := time.Parse("2006_01_02_15", timestampStr)
		if err != nil {
			Log(ctx, logr, "error", fmt.Sprintf("[archive-manager-phpc] error parsing collection timestamp for %s: %v", collectionName, err))
			continue
		}

		// if collectionTime is in the same hour as current hour, then skip
		if collectionTime.Hour() == time.Now().Hour() {
			continue
		}

		// Process the collection - fetch docs and archive
		zeroResultsCount := 0
		for {
			shutdownMutex.Lock()
			if Shutdown {
				shutdownMutex.Unlock()
				fmt.Printf("[stream] shutdown signal received- flushing buffer\n")
				if err := st.FlushBuffer(); err != nil {
					Log(ctx, logr, "error", fmt.Sprintf("[archive-manager-phpc][archiveBuffer] error: %v\n", err))
				}
				threshold = 0
				return nil
			}
			shutdownMutex.Unlock()

			docs, err := st.fetchDocumentsFromMongo(ctx, logr, mongoDB, redisClient, collectionName)
			if err != nil {
				// dont add in processedCollections. As its partially processed.
				Log(ctx, logr, "error", fmt.Sprintf("[archive-manager-phpc] Failed to fetch docs: %v", err))
				return err
			}
			if len(docs) == 0 {
				Log(ctx, logr, "info", fmt.Sprintf("[archive-manager-phpc] No docs found in collection %s", collectionName))
				zeroResultsCount++
				// Retry 3 times before breaking.
				// When all docs are processed, the next collection will be processed. so break.
				if zeroResultsCount >= 1 {
					if err := st.FlushBuffer(); err != nil {

						fmt.Printf("[archive-manager-phpc][archiveBuffer] error: %v\n", err)
						syscall.Kill(syscall.Getpid(), syscall.SIGINT)
						return err
					}
					Log(ctx, logr, "info", fmt.Sprintf("[archive-manager-phpc] No docs found in collection %s. moving to next collection", collectionName))
					threshold = 0
					break
				}
				time.Sleep(MongoFetchInterval())
				continue
			}

			docsBufferMutex.Lock()
			st.Docs = append(st.Docs, docs...)
			docsBufferMutex.Unlock()

			threshold += len(docs)

			if threshold >= int(BufferLimit()) || (time.Now().Unix()-lastFlushTime.Unix()) > int64(BufferFlushDuration().Seconds()) {
				if err := st.FlushBuffer(); err != nil {
					fmt.Printf("[archive-manager-phpc][archiveBuffer] error: %v\n", err)

					syscall.Kill(syscall.Getpid(), syscall.SIGINT)
					return err
				}
				threshold = 0
			}
			time.Sleep(MongoFetchInterval())
		}

		if err := st.FlushBuffer(); err != nil {
			fmt.Printf("[archive-manager-phpc][archiveBuffer] error: %v\n", err)

			syscall.Kill(syscall.Getpid(), syscall.SIGINT)
			return err
		}
		threshold = 0

		// Add collection to Redis sorted set with its timestamp as the score
		_, err = redisClient.GetClient().ZAdd(ctx, processedCollectionsSetKey, r.Z{
			Score:  float64(collectionTime.Unix()),
			Member: collectionName,
		}).Result()
		if err != nil {
			Log(ctx, logr, "error", fmt.Sprintf("[archive-manager-phpc] Error adding collection %s to processed set: %v", collectionName, err))
		}

		// drop collection from mongo
		err = mongoDB.Collection(collectionName).Drop(ctx)
		if err != nil {
			Log(ctx, logr, "error", fmt.Sprintf("[archive-manager-phpc] Error dropping collection %s: %v", collectionName, err))
		}
		// delete processed collection from redis
		_, err = redisClient.GetClient().ZRem(ctx, processedCollectionsSetKey, collectionName).Result()
		if err != nil {
			Log(ctx, logr, "error", fmt.Sprintf("[archive-manager-phpc] Error deleting collection %s from processed set: %v", collectionName, err))
		}
		// log
		Log(ctx, logr, "info", fmt.Sprintf("[archive-manager-phpc] Dropped collection %s and deleted from processed set", collectionName))
	}

	// Todo-Lets process the current hour collections until next hours 2minutes are passed. Than switch to next hour.
	// Gained control of the lock

	return nil
}

/**
*********************** Archive Buffer ***************************
**/
func (st *Archiver) FlushBuffer() error {
	if len(st.Docs) <= 0 {
		Log(ctx, logr, "info", "[archiveBuffer] No activities to insert \n")
		return nil
	}
	Log(ctx, logr, "info", fmt.Sprintf("[archiveBuffer] Last processed ID: %s\n", lastID.Hex()))

	appDir := os.Getenv("APP_DIR")
	if appDir == "" {
		return fmt.Errorf("APP_DIR is not set")
	}

	fileName := fmt.Sprintf("d_%s.csv", time.Now().Format("20060102_150405"))
	filePath := fmt.Sprintf("%s/data/%s/%s", appDir, st.Name, fileName)

	// write docs to csv
	err := st.Writer.WriteDocumentsToCSV(filePath, st.Docs)
	if err != nil {
		return err
	}

	objectName := fmt.Sprintf("archive/%s/%s", st.Name, fileName)
	// upload csv to gcs
	err = gcp.UploadFileToGCS(ctx, os.Getenv("GCP_BUCKET_NAME"), objectName, filePath, "text/csv")
	if err != nil {
		return err
	}

	// delete csv file
	err = os.Remove(filePath)
	if err != nil {
		return err
	}

	docsBufferMutex.Lock()
	// Todo- write a custom sort function for this.
	// reflection is costly.
	slices.SortFunc(st.Docs, func(a, b interface{}) int {
		// compare two mongodb object ID timestamps
		// careful with type assertion. ->
		return mongodb.CompareObjectIDTimestamps(a.(bson.D)[0].Value.(primitive.ObjectID), b.(bson.D)[0].Value.(primitive.ObjectID))
	})
	docsBufferMutex.Unlock()

	// select the first and last id
	firstID := st.Docs[0].(bson.D)[0].Value.(primitive.ObjectID)
	lastID := st.Docs[len(st.Docs)-1].(bson.D)[0].Value.(primitive.ObjectID)
	// delete docs from  mongo
	err = deleteDocumentsFromMongo(ctx, mongoClient, firstID, lastID, GetMongoCollectionName())
	if err != nil {
		return err
	}

	err = st.setLastProcessedID(redisClient, lastID.Hex())
	if err != nil {
		Log(ctx, logr, "error", fmt.Sprintf("[archiveBuffer] Failed to Flush last processed ID: %v", err))
	}
	Log(ctx, logr, "info", fmt.Sprintf("[archiveBuffer] Updated last processed ID: %s\n", lastID.Hex()))
	lastFlushTime = time.Now()
	Log(ctx, logr, "info", fmt.Sprintf("[archiveBuffer] Archived %d docs \n", len(st.Docs)))

	docsBufferMutex.Lock()
	st.Docs = nil
	docsBufferMutex.Unlock()

	return nil
}

// setLastProcessedID is a helper function to set the last processed ID in Redis
func (st *Archiver) setLastProcessedID(client *redis.Client, id string) error {
	tx := client.GetClient().TxPipeline()
	tx.Set(ctx, GetLastIDRedisKey(), id, 0)
	_, err := tx.Exec(ctx)
	return err
}

/**
*********************** Fetch Documents From Mongo ***************************
**/
// Fetch docs from mongo. Updates local inmem lastID.
func (st *Archiver) fetchDocumentsFromMongo(ctx context.Context, logr *logger.CustomLogger, mongoDB *mongo.Database, redisClient *redis.Client, collName string) ([]interface{}, error) {
	// recoverer
	defer func() {
		if r := recover(); r != nil {
			errStr := fmt.Sprintf("[fetchDocumentsFromMongo] Failed to fetch docs: %v", r)
			print(errStr)
			// sentry.CaptureException(r)
			Log(ctx, logr, "error", fmt.Sprintf("[fetchDocumentsFromMongo] Failed to fetch docs: %v", r))
		}
	}()

	var err error
	if lastID == primitive.NilObjectID {
		lastID, err = getLastProcessedID(ctx, logr, redisClient)
		if err != nil {

			Log(ctx, logr, "error", fmt.Sprintf("[fetchDocumentsFromMongo] Failed to get last processed ID: %v", err))
			return nil, err
		}
	}

	collection := mongoDB.Collection(collName)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	maxObjectID := getMaximumObjectID(GetMaxID())

	// Add the filter conditions for both $gte lastID and $lte minObjectID
	filter := bson.M{
		"_id": bson.M{
			"$gte": lastID,      // Ensure doc inserted for same second is not missed
			"$lt":  maxObjectID, // Ensure only docs within the maxID or last x duration are fetched - both are configured from env
		},
	}
	extraFilte, err := st.Writer.ExtraFilter()
	if err != nil {
		return nil, err
	}
	// merge extra filter
	for k, v := range extraFilte {
		filter[k] = v
	}

	options := options.Find().SetLimit(int64(MongoFetchLimit())).SetSort(bson.D{{Key: "_id", Value: 1}})
	cursor, err := collection.Find(ctx, filter, options)
	if err != nil {

		return nil, err
	}
	defer cursor.Close(ctx)

	var docs []interface{}
	if err := cursor.All(ctx, &docs); err != nil {

		return nil, err
	}

	// Filter out the lastID doc from the list
	docs = lo.Filter(docs, func(doc interface{}, _ int) bool {
		docMap, ok := doc.(bson.D)
		if !ok {
			// This should never happen
			sentry.CaptureException(fmt.Errorf("[Filter out the lastID ] doc is not of type bson.D"))
			panic(fmt.Errorf("[Filter out the lastID ] doc is not of type bson.D"))
			// return false
		}
		id, ok := docMap[0].Value.(primitive.ObjectID)
		if !ok {
			sentry.CaptureException(fmt.Errorf("[Filter out the lastID ] id is not of type primitive.ObjectID"))
			panic(fmt.Errorf("[Filter out the lastID ] id is not of type primitive.ObjectID"))
			// return false
		}
		return id != lastID
	})

	if len(docs) == 0 {
		Log(ctx, logr, "info", "[fetchDocumentsFromMongo] No new docs found\n")
		return nil, nil
	}

	lastDoc, ok := docs[len(docs)-1].(bson.D)
	if !ok {
		sentry.CaptureException(fmt.Errorf("lastDoc is not of type bson.D"))
		panic(fmt.Errorf("lastDoc is not of type bson.D"))
	}
	lastID, ok = lastDoc[0].Value.(primitive.ObjectID)
	if !ok {
		sentry.CaptureException(fmt.Errorf("lastID is not of type primitive.ObjectID"))
		panic(fmt.Errorf("lastID is not of type primitive.ObjectID"))
	}

	Log(ctx, logr, "info", fmt.Sprintf("[fetchDocumentsFromMongo] last docID: %s\n", lastID.Hex()))
	return docs, nil
}

// getLastProcessedID retrieves the last processed MongoDB ObjectID from Redis
// It uses a Redis key defined by GetLastIDRedisKey() to track processing state
func getLastProcessedID(ctx context.Context, logr *logger.CustomLogger, client *redis.Client) (primitive.ObjectID, error) {
	val, err := client.GetClient().Get(ctx, GetLastIDRedisKey()).Result()
	if err == r.Nil {
		Log(ctx, logr, "warn", "[getLastProcessedID] Warning! First Run")
		return primitive.NilObjectID, nil // No value, assume this is the first run
	} else if err != nil {
		return primitive.NilObjectID, err
	}

	id, err := primitive.ObjectIDFromHex(val)
	if err != nil {
		return primitive.NilObjectID, err
	}

	return id, nil
}

// deleteDocumentsFromMongo deletes a range of documents from MongoDB collection
// WARNING: This operation is irreversible and will permanently delete data
func deleteDocumentsFromMongo(ctx context.Context, client *mongo.Client, fId, lId primitive.ObjectID, collName string) error {
	collection := client.Database(mongodb.GetDefaultDbName(os.Getenv("DEFAULT_DB_NAME"))).Collection(collName)
	_, err := collection.DeleteMany(ctx, bson.M{"_id": bson.M{"$gte": fId, "$lte": lId}})
	if err != nil {
		return err
	}

	return nil
}

// getMaximumObjectID returns the maximum object ID based on the UPTO duration
// If MAX_ID found from env we return that id so that : we can archive from lastProcessed Id from redis to MAX_ID found from Env
func getMaximumObjectID(maxID string) primitive.ObjectID {
	// if MAX_ID found from env we return that id
	//so that : we can insert doc from lastProcessed Id from redis to MAX_ID found from Env
	if len(maxID) == 24 {
		return mongodb.GetOptimisticObjectIdFromHex(maxID)
	}

	// Calculate the timestamp for UPTO duration ago
	uptoTime := time.Now().Add(-1 * uptoDuration)
	// Create an ObjectID from the oneMinuteAgo timestamp
	maxObjectId := primitive.NewObjectIDFromTimestamp(uptoTime)
	return maxObjectId
}
