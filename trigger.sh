#!/usr/bin/env bash

token='je8jYUctZxs_CvGiblNg3w'
body='{ "request": {
		"message": "Override the commit message: this is an api request",
		"branch":"master",
		"config":{
			"env": {"TRIGGER": "yes"}
		}
	}}'

repos=(
    'microservice-user'
    'microservice-user-profile'
    'microservice-schema-management'
    'microservice-registration'
    'microservice-metadata'
    'microservice-mail'
    'microservice-apps-management'
    'microservice-namespaces'
    'jwt-issuer'
    'identity-provider'
    'authorization-server'
)

for repo in "${repos[@]}"; do
	curl -so /dev/null -X POST \
		-w "%{http_code}" \
		-H "Content-Type: application/json" \
		-H "Accept: application/json" \
		-H "Travis-API-Version: 3" \
		-H "Authorization: token $token" \
		-d "$body" \
		"https://api.travis-ci.com/repo/JormungandrK%2F${repo}/requests"
done