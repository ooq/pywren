for c in 3000 3500 4000 4500 5000 5500 6000 6500 7000 7500 8000
#for c in 1 2
do
SECONDS=0;
python limit_exp.py $c
echo "runtime for "$c" is " $SECONDS >> runtimes.txt
if [ $SECONDS -lt 300 ];
then
	let t=300-SECONDS
	echo "sleep for "$t
	sleep $t
fi
done
