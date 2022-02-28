#! /bin/bash

for i in {0..100}
  do
  echo "$i"
  name=test-$i
  echo "$i" > logs/2CLOGS/$name.txt
  go test -run 2C -race > logs/2CLOGS/$name.txt
done