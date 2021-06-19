#!/bin/bash

url=http://localhost:8080/orders
total=100

for i in $(seq 1 $total);do
  identifier=$RANDOM
  customer="Customer $(((RANDOM % 10 )+ 1))"
  value=$RANDOM

  echo '---------'

  curl -s -X POST ${url} \
    -H "Content-Type: application/json" \
    -d "{\"identifier\": \"${identifier}\",\"customer\": \"${customer}\",\"value\": ${value}}"

done