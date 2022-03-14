#! /bin/bash

for i in {0..100}
  do
  echo "$i"
  name=test-$i
  echo "$i" > logs/2BLOGS/$name.txt
  go test -run 2B -race > logs/2BLOGS/$name.txt
done