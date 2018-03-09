# backends
A package that supports multiple backends( MongoDB, DynamoDB )

# Service configuration

The service loads the configuration from a JSON. 
Here's an example of a JSON configuration file for DB settings:

```json
{
  "database":{
    "dbName": "dynamodb",
    "dbInfo": {
      "credentials": "/run/secrets/aws-credentials",
      "endpoint": "http://dynamo:8000",
      "awsRegion": "us-east-1",
      "host": "mongo:27017",
      "database": "users",
      "collections": {
        "users": {"indexes": ["email"], "enableTTL": false, "hashKey": "email", "readCapacity": 5, "writeCapacity": 5},
        "tokens": {"indexes": ["token"], "enableTTL": true, "TTL": 86400, "hashKey": "token", "readCapacity": 5, "writeCapacity": 5}
      },
      "user": "restapi",
      "pass": "restapi"
    }
  }
}
```

Configuration properties:
 * **dbName** - ```"dynamodb/mongodb"``` - is the name of the database( it can be mongodb/dynamodb ).
 * **dbInfo** - holds informations about each database.
 * **credentials** - ```"/run/secrets/aws-credentials"``` - is the full the to the AWS credentials file.
 * **endpoint** - ```"http://dynamo:8000"``` - is the dynamoDB endpoint. Format http://host:port
 * **awsRegion** - ```us-east-1``` - is the AWS region.
 * **host** - ```mongo:27017``` - mongoDB endpoint. Format host:port.
 * **database** - ```users``` - database name. Use only for mongoDB.
 * **collections** - defines the collections/tables with indexes and other properties
 * **user** - mongo database user
 * **pass** - mongo database password


