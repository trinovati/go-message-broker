#!/bin/sh
for OUTPUT in $(find ./ -name *_test.go )
do
    echo $OUTPUT
    RESULT=$(go test $OUTPUT)
    if echo "$RESULT" | grep "FAIL"; then
        echo "$RESULT"
        exit 1
    else 
        echo "OK\n\n"
    fi
done