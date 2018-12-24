package main

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

func sendCommit(diff string) (error) {

	// Transform the dynamoDBUser struct in the attribute map
	av, err := dynamodbattribute.MarshalMap(diff)
	if err != nil {
		return err
	}

	// Start a session with DynamoDB and put the item
	svcDynamo := dynamodb.New(session.New())
	input := &dynamodb.PutItemInput{
		Item:                av,
		TableName:           aws.String("users"),
		ConditionExpression: aws.String("attribute_not_exists(email)"),
	}

	// Make the query and check for errors
	_, err = svcDynamo.PutItem(input)

	return err
}

func checkRepoExistence(repoName string) (bool, error) {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Config: aws.Config{Region: aws.String("sa-east-1")},
		Profile: "blmayer",
	}))

	svc := dynamodb.New(sess)

	// Check if table exists
	tables, err := svc.ListTables(&dynamodb.ListTablesInput{})
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
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Config: aws.Config{Region: aws.String("sa-east-1")},
		Profile: "blmayer",
	}))

	svc := dynamodb.New(sess)

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

	_, err := svc.CreateTable(input)

	if err != nil {
		return err
	}

	return nil
}

