#!/bin/bash
for((i=0;i<$1;++i)) do
	go test -run $2
	if [ $? -ne 0 ]; then
		break
	fi
done
