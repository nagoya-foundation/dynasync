package main

import (
	"fmt"
	"time"
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

func sendCommit(commit Commit) (error) {
	av, err := dynamodbattribute.MarshalMap(commit)
	if err != nil {
		return err
	}

	input := &dynamodb.PutItemInput{
		Item:                av,
		ConditionExpression: aws.String("attribute_not_exists(commitDate)"),
		TableName:           aws.String("commits"),
	}

	// Make the query and check for errors
	_, err = DYNAMODB.PutItem(input)

	return err
}

func getCommit(date int64) (Commit, error) {
	dateInt := fmt.Sprintf("%d", date)

	result, err := DYNAMODB.GetItem(
		&dynamodb.GetItemInput{
			Key: map[string]*dynamodb.AttributeValue{
				"commitDate": {
					N: aws.String(dateInt),
				},
			},
			TableName: aws.String("commits"),
		},
	)

	if err != nil {
		return Commit{}, err
	}

	var commit Commit

	err = dynamodbattribute.UnmarshalMap(result.Item, &commit)

	if err != nil {
		return Commit{}, err
	}

	return commit, nil
}

func listRepoCommits() ([]int64, error) {
	input := dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"repo": {
				S: aws.String(REPONAME),
			},
		},
		ProjectionExpression: aws.String("commits"),
		TableName:            aws.String("repos"),
	}

	// TODO: Implement pagination
	result, err := DYNAMODB.GetItem(&input)

	if err != nil {
		return nil, err
	}

	var repo RepoIndex

	err = dynamodbattribute.UnmarshalMap(result.Item, &repo)

	if err != nil {
		return nil, err
	}

	return repo.Commits, nil
}

func updateIndex(commit Commit) (error) {
	date := fmt.Sprintf("%d", commit.Date)
	input := &dynamodb.UpdateItemInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":c": {
				NS: []*string{aws.String(string(date))},
			},
			":f": {
				SS: []*string{aws.String(commit.File)},
			},
		},
		Key: map[string]*dynamodb.AttributeValue{
			"repo": {
				S: aws.String(REPONAME),
			},
		},
		ReturnValues:     aws.String("NONE"),
		UpdateExpression: aws.String("add files :f,commits :c"),
		TableName:        aws.String("repos"),
	}
	_, err := DYNAMODB.UpdateItem(input)

	// Make the query and check for errors
	return err
}

func tag(msg string) (error) {
	// Build Commit struct
	data := Tag{
	//	Date: time.Now().Unix(),
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

func createRepo() (error) {
	// Check if table exists
	tables, err := DYNAMODB.ListTables(&dynamodb.ListTablesInput{})
	if err != nil {
		return err
	}
	hasRepos := false
	hasCommits := false
	for _, table := range tables.TableNames {
		if *table == "repos" {
			hasRepos = true
		}
		if *table == "commits" {
			hasCommits = true
		}
	}

	if !hasRepos {
		_, err = DYNAMODB.CreateTable(
			&dynamodb.CreateTableInput{
				AttributeDefinitions: []*dynamodb.AttributeDefinition{
					{
						AttributeName: aws.String("repo"),
						AttributeType: aws.String("S"),
					},
				},
				KeySchema: []*dynamodb.KeySchemaElement{
					{
						AttributeName: aws.String("repo"),
						KeyType:       aws.String("HASH"),
					},
				},
				ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(25),
					WriteCapacityUnits: aws.Int64(25),
				},
				TableName: aws.String("repos"),
			},
		)
	}
	if err != nil {
		return err
	}

	if !hasCommits {
		_, err = DYNAMODB.CreateTable(
			&dynamodb.CreateTableInput{
				AttributeDefinitions: []*dynamodb.AttributeDefinition{
					{
						AttributeName: aws.String("filePath"),
						AttributeType: aws.String("S"),
					},
					{
						AttributeName: aws.String("commitDate"),
						AttributeType: aws.String("N"),
					},
				},
				KeySchema: []*dynamodb.KeySchemaElement{
					{
						AttributeName: aws.String("filePath"),
						KeyType:       aws.String("HASH"),
					},
					{
						AttributeName: aws.String("commitDate"),
						KeyType:       aws.String("RANGE"),
					},
				},
				ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(25),
					WriteCapacityUnits: aws.Int64(25),
				},
				TableName: aws.String("commits"),
			},
		)
		time.Sleep(5 * time.Second)
	}
	if err != nil {
		return err
	}

	// Now add the repo item
	repo := RepoIndex{
		Repo:         REPONAME,
		Files:        []string{},
		Commits:      []int64{},
		CreationDate: time.Now().Unix(),
		Owner:        AUTHOR,
	}

	av, err := dynamodbattribute.MarshalMap(repo)
	if err != nil {
		return err
	}

	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String("repos"),
	}

	// Make the query and check for errors
	_, err = DYNAMODB.PutItem(input)
	if err != nil {
		return err
	}

	return nil
}
