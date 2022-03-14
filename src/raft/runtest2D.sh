#! /bin/bash

for i in {0..50}
  do
  echo "$i"
  name=test-$i
  echo "$i" > logs/2DLOGS/$name.txt
  go test -run 2D -race > logs/2DLOGS/$name.txt
done