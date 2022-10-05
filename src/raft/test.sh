#!/bin/bash
for((i=0;i<$2;++i)) do
	go test -race -run $1
	if [ $? -ne 0 ]; then
		break
	fi
done
