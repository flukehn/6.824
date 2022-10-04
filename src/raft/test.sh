#!/bin/bash
for((i=0;i<10;++i)) do
	go test -race -run 2B
	if [ $? -ne 0 ]; then
		break
	fi
done
