#!/bin/bash

echo "Starting sumo_receiver"
echo "Looking for existing process"

pid=$(ps aux | grep "python ../bin/scripts/wf_sumo_receiver.py" | grep -v grep | awk '{print $2}')

if [[ $pid ]] 
then
    echo "Existing process found: $pid"
    echo "Stopping.."
    kill $pid  
else
    echo "No existing process found"
fi

echo "Starting sumo_receiver background process"

setsid ../bin/scripts/wf_sumo_receiver.py $1 $2 $3 >/private/adnj/Desktop/output.log 2>&1 < /dev/null &