package main

import (
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

func sendCommit(repoName string, diff string) (error) {

	av, err := dynamodbattribute.MarshalMap(diff)
	if err != nil {
		return err
	}

	input := &dynamodb.PutItemInput{
		Item:                av,
		TableName:           aws.String(repoName),
		ConditionExpression: aws.String("attribute_not_exists(email)"),
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

func createRepo(repoName string) (error) {
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("commit"),
				AttributeType: aws.String("N"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("commit"),
				KeyType:       aws.String("HASH"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(10),
		},
		TableName: aws.String(repoName),
	}

	_, err := DYNAMODB.CreateTable(input)

	if err != nil {
		return err
	}

	return nil
}

