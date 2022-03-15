#! /bin/bash

for i in {0..50}
  do
  echo "$i"
  name=test-$i
  echo "$i" > logs/LOGS/$name.txt
  go test -race > logs/LOGS/$name.txt
done