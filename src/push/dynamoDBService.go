package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"time"
)

func startDynamoDBSession() *dynamodb.DynamoDB {

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

func sendCommit(commit Commit) error {
	av, err := dynamodbattribute.MarshalMap(commit)
	if err != nil {
		return err
	}

	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String("commits"),
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

func listRepoCommits() ([]Commit, error) {
	input := dynamodb.QueryInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":v1": {
				S: aws.String(REPONAME),
			},
		},
		KeyConditionExpression: aws.String("repo = :v1"),
		TableName:              aws.String("commits"),
	}

	// TODO: Implement pagination
	result, err := DYNAMODB.Query(&input)

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

func updateIndex(commit Commit) error {
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

func tag(msg string) error {
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

func createRepo() error {
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

	if !hasCommits || !hasRepos {
		panic("DynamoDB tables not found, make sure you done" +
			"the configuration steps in README.md file")
	}

	// Add the repo item
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
