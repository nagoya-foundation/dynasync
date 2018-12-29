package main

import (
	"time"
	"errors"
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

func sendCommit(file string, hash [16]byte, diff string, msg string) (error) {
	// Before everything, check if the table exists
	hasRepo, err := checkRepoExistence(REPONAME)
	if err != nil {
		return err
	} else if !hasRepo {
		return errors.New("commit failed: Table " + REPONAME + " not found")
	}

	// Build Commit struct
	data := Commit{
		File:    file,
		Date:    time.Now().Unix(),
		Diff:    diff,
		Hash:    hash,
		Message: msg,
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

func getAllCommits(repo string) ([]Commit, error) {

	input := &dynamodb.ScanInput{
		TableName: aws.String(repo),
	}

	// TODO: Implement pagination
	result, err := DYNAMODB.Scan(input)

	if err != nil {
		return nil, err
	}

	var commits []Commit

	err = dynamodbattribute.UnmarshalListOfMaps(result.Items, &commits)

	if err != nil {
		return nil, err
	}

	return commits, nil
}

func tag(msg string) (error) {
	// Before everything, check if the table exists
	hasRepo, err := checkRepoExistence(REPONAME)
	if err != nil {
		return err
	} else if !hasRepo {
		return errors.New("tag failed: Table " + REPONAME + " not found")
	}

	// Build Commit struct
	data := Tag{
		Date: time.Now().Unix(),
		File: "tagFile",
		Text: msg,
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
				AttributeName: aws.String("date"),
				AttributeType: aws.String("N"),
			},
			{
				AttributeName: aws.String("filePath"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("date"),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String("filePath"),
				KeyType:       aws.String("RANGE"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(25),
			WriteCapacityUnits: aws.Int64(25),
		},
		TableName: aws.String(REPONAME),
	}

	_, err := DYNAMODB.CreateTable(input)

	if err != nil {
		return err
	}

	return nil
}

