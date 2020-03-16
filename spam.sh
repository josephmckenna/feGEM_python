#/bin/bash

echo "Warning, I will close all programs running in this terminal if sourced!"
sleep 1

NUMBER_OF_SESSION=${1}
RUN_FOR_N_SECONDS=${2}
if [ `echo "$NUMBER_OF_SESSION" | wc -c` -gt 1 ]; then
  echo '$NUMBER_OF_SESSION set from terminal'
else
  NUMBER_OF_SESSION=10
fi
echo "Running $NUMBER_OF_SESSION parrallel python packers"

if [ `echo "$RUN_FOR_N_SECONDS" | wc -c` -gt 1 ]; then
  echo '$RUN_FOR_N_SECONDS set from terminal'
else
  RUN_FOR_N_SECONDS=30
fi
echo "Running for $RUN_FOR_N_SECONDS seconds before killing"

for i in `seq 1 5 | tac`; do
  echo "Starting in ${i}"
  sleep 1
done



for i in `seq 1 ${NUMBER_OF_SESSION}`; do 
python3 python_packer.py &  
done && sleep ${RUN_FOR_N_SECONDS} && for i in `seq 1 ${NUMBER_OF_SESSION}`; do
kill %${i}; 
done
