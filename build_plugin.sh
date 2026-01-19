#!/bin/bash
echo "Building DynamoDB Plugin..."
go build -o noxy-plugin-dynamodb .
echo "Done. Created ./noxy-plugin-dynamodb"
