#! /bin/bash

for i in {0..10}
  do
  echo "$i"
  name=test-$i
  echo "$i" > logs/2DLOGS/$name.txt
  go test -run SnapshotInstallUnreliable2D -race > logs/2DLOGS/$name.txt
done