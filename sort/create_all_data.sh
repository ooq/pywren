#!/bin/bash
for i in `seq 0 9999`;
do
        echo $i
        time (python pywren-part-s3.py $i 1 1667)
done    
