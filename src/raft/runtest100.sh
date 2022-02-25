#! /bin/bash

for i in {0..100}
  do
  echo "$i"
  name=test-$i
  echo "$i" > logs/$name.txt
  go test -run 2C -race > logs/$name.txt
done