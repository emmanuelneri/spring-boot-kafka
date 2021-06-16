#!/bin/bash

url=http://localhost:8081/subjects

schemas=$(curl -s ${url})
echo "All schemas: ${schemas}"

echo "${schemas}" | jq -c -r '.[]' | while read schema; do
  echo "--------------------------${schema}------------------------------------------"
  echo $(curl -s ${url}/${schema}/versions/latest) | jq .
done