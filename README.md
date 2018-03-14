# backends
A package that supports multiple backends( MongoDB, DynamoDB )

## Use in Goa

In the ```main.go```:

Define the supported backends:

```go
  backendManager := backends.NewBackendSupport(map[string]*config.DBInfo{
    "mongodb":  &dbConf.DBInfo,
    "dynamodb": &dbConf.DBInfo,
  })

```

Get the desire backend(mongoDB or dynamoDB):

```go
  backend, err := backendManager.GetBackend(dbConf.DBName)
  if err != nil {
    service.LogError("Failed to configure backend. ", err)
  }
```

Define the repositories(collections/tables):

```go
  userRepo, err := backend.DefineRepository("users", backends.RepositoryDefinitionMap{
    "name":          "users",
    "indexes":       []string{"email"},
    "hashKey":       "email",
    "readCapacity":  int64(5),
    "writeCapacity": int64(5),
    "GSI": map[string]interface{}{
      "email": map[string]interface{}{
        "readCapacity":  1,
        "writeCapacity": 1,
      },
    },
  })
  if err != nil {
    service.LogError("Failed to get users repo.", err)
    return
  }

  tokenRepo, err := backend.DefineRepository("tokens", backends.RepositoryDefinitionMap{
    "name":          "tokens",
    "indexes":       []string{"token"},
    "hashKey":       "token",
    "readCapacity":  int64(5),
    "writeCapacity": int64(5),
    "GSI": map[string]interface{}{
      "token": map[string]interface{}{
        "readCapacity":  1,
        "writeCapacity": 1,
      },
    },
    "enableTtl": true,
    "ttlAttribute":  "created_at",
    "ttl":       86400,
  })
  if err != nil {
    service.LogError("Failed to get tokens repo.", err)
    return
  }
```

* **name** - is the name of the collection/table
* **indexes** - are the mongoDB indexs
* **hashKey** - is the primary key (hash key) for dynamoDB table
* **rangeKey** - is the sort key (range key) for dynamoDB table
* **readCapacity** - is the read capacity of the table. 1 unit is eqaul to 4KB
* **writeCapacity** - is the write capacity of the table. 1 unit is eqaul to 4KB
* **GSI** - are the global secondary indexes for dynamoDB
* **enableTtl** - set TTL
* **ttlAttribute** - is the TTL attribute in the collection/table
* **ttl** - is the TTL value in seconds

Then define the store and pass it to the controller:

```go
  // outside the main.go
  // User wraps User's collections/tables. Implements backneds.Repository interface
  type User struct {
    Users  backends.Repository
    Tokens backends.Repository
  }


  ...

  // in the main.go
  store := store.User{
    userRepo,
    tokenRepo,
  }

  // Mount "user" controller
  c2 := NewUserController(service, store)
  app.MountUserController(service, c2)
```

## Service configuration

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
 * **user** - mongo database user
 * **pass** - mongo database password


