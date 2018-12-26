import (
	"fmt"
	"time"
	"errors"
	"crypto/md5"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

func startDynamoDBSession() (*dynamodb.DynamoDB) {

	// Start a session with DynamoDB
	sess := session.Must(
		session.NewSessionWithOptions(
			session.Options{
				Config: aws.Config{
					Region: aws.String(AWSREGION),
				},
				Profile: AWSPROFILE,
			},
		),
	)

	return dynamodb.New(sess)
}

func sendCommit(diff string, message string) (error) {
	// Before everything, check if the table exists
	hasRepo, err := checkRepoExistence(REPONAME)
	if err != nil {
		return err
	} else if !hasRepo {
		return errors.New("Commit failed: Table " + REPONAME + " not found")
	}

	hash := md5.Sum([]byte(diff))

	// Build Commit struct
	data := Commit{
		Id:      hash[:16],
		Date:    time.Now().Unix(),
		Diff:    diff,
		Message: message,
	}

	av, err := dynamodbattribute.MarshalMap(data)
	if err != nil {
		return err
	}

	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(REPONAME),
	}

	// Make the query and check for errors
	_, err = DYNAMODB.PutItem(input)

	return err
}

func checkRepoExistence(repoName string) (bool, error) {
	// Check if table exists
	tables, err := DYNAMODB.ListTables(&dynamodb.ListTablesInput{})
	if err != nil {
		return false, err
	}
	for _, repo := range tables.TableNames {
		if *repo == repoName {
			return true, nil
		}
	}

	return false, nil
}

func createRepo() (error) {
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("commitId"),
				AttributeType: aws.String("B"),
			},
			{
				AttributeName: aws.String("date"),
				AttributeType: aws.String("N"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("commitId"),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String("date"),
				KeyType:       aws.String("RANGE"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(10),
		},
		TableName: aws.String(REPONAME),
	}

	_, err := DYNAMODB.CreateTable(input)

	if err != nil {
		return err
	}

	return nil
}

