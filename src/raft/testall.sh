#!/bin/bash
for((i=0;i<$1;++i)) do
	go test
	if [ $? -ne 0 ]; then
		break
	fi
done
