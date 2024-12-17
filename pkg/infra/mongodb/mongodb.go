package mongodb

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/samber/lo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type MongoClient struct {
	client *mongo.Client
}

type MongoDefaultDB struct {
	db *mongo.Database
}

// Function to fetch mongo connection params from env variables and create and than return mongodb client.
func NewMongoClient(uri string) (*MongoClient, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}
	// Test the connection to MongoDB
	if err := client.Ping(ctx, nil); err != nil {
		fmt.Println("Failed to connect to MongoDB:", err)
		return nil, err
	}
	fmt.Println("Connected to MongoDB")
	return &MongoClient{client: client}, nil
}

// Function to close mongodb client.
func (mc *MongoClient) Close(ctx context.Context) {
	err := mc.client.Disconnect(ctx)
	if err != nil {
		fmt.Println("Failed to disconnect from MongoDB:", err)
		return
	}
	fmt.Println("Disconnected from MongoDB")
}

// Function to get the default database from the mongo client.
func (mc *MongoClient) GetDefaultDB(dbName string) *mongo.Database {
	return mc.client.Database(GetDefaultDbName(dbName))
}

func Find(DefaultMongoDb *MongoDefaultDB, tableName string, filter bson.M, projection *options.FindOptions) ([]interface{}, error) {
	collection := DefaultMongoDb.db.Collection(tableName)
	cur, err := collection.Find(context.Background(), filter, projection, options.Find().SetMaxTime(1*time.Minute))
	var result []interface{}
	if err != nil {
		return result, err
	}
	defer cur.Close(context.Background())
	for cur.Next(context.Background()) {
		var doc interface{}
		err := cur.Decode(&doc)
		if err != nil {
			return result, err
		}
		result = append(result, doc)
	}
	return result, nil
}

func FindOne(DefaultMongoDb *MongoDefaultDB, tableName string, filter bson.M, projection *options.FindOneOptions) (interface{}, error) {
	collection := DefaultMongoDb.db.Collection(tableName)
	var result interface{}
	err := collection.FindOne(context.Background(), filter, projection).Decode(&result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func ConvertStringToObjectIds(Ids []string) []primitive.ObjectID {
	objIds := []primitive.ObjectID{}
	objIds = lo.Map(Ids, func(x string, index int) primitive.ObjectID {
		objID, err := primitive.ObjectIDFromHex(x)
		if err != nil {
			panic(err)
		}
		return objID
	})
	return objIds
}

// Only when you are sure that the id is valid and used for getting the object from the database.
func GetOptimisticObjectIdFromHex(id string) primitive.ObjectID {
	objID, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		panic(err) //Care Full
		// return primitive.NilObjectID
		// return new primitive.ObjectID()
		// return primitive.ObjectID{}
	}
	return objID
}

// Helper function to check if a BulkWriteException is due to duplicate key errors
func IsDuplicateKeyError(err error) bool {
	writeException, ok := err.(mongo.BulkWriteException)
	if ok {
		for _, writeError := range writeException.WriteErrors {
			if writeError.Code == 11000 { // 11000 is the MongoDB error code for duplicate key
				return true
			}
		}
	}
	return false
}

func ConvertStringSliceToObjectIdSlice(strSlice []string) []primitive.ObjectID {
	var objIds []primitive.ObjectID
	for _, str := range strSlice {
		objId, _ := primitive.ObjectIDFromHex(str)
		objIds = append(objIds, objId)
	}
	return objIds
}

func ConvertObjectIdSliceToStringSlice(objIds []primitive.ObjectID) []string {
	var strIds []string
	for _, objId := range objIds {
		strIds = append(strIds, objId.Hex())
	}
	return strIds
}

// func returns whether given id is valid mongo id or not
func IsValidMongoId(id string) bool {
	if len(id) != 24 {
		return false
	}
	_, err := primitive.ObjectIDFromHex(id)
	return err == nil
}

// returns -1 if idA is before idB, 1 if idA is after idB, 0 if they are equal
func CompareObjectIDTimestamps(idA, idB primitive.ObjectID) int {
	// Compare the timestamps embedded in the ObjectIDs
	if idA.Timestamp().Before(idB.Timestamp()) {
		return -1
	} else if idA.Timestamp().After(idB.Timestamp()) {
		return 1
	}
	return 0 // timestamps are equal
}

func GetDefaultDbName(dbName string) string {
	if dbName == "" {
		dbName = "test"
	}
	return dbName
}
