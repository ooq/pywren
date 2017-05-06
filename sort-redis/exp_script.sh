export redisnode1=pywren-redis-1b.oapxhs.clustercfg.usw2.cache.amazonaws.com
export redisnode2=pywren-redis-10c.oapxhs.clustercfg.usw2.cache.amazonaws.com
export redisnode3=pywren-redis-10d.oapxhs.clustercfg.usw2.cache.amazonaws.com
#export redisnode4=pywren-redis-10e.oapxhs.clustercfg.usw2.cache.amazonaws.com
#export redisnode5=pywren-redis-10f.oapxhs.clustercfg.usw2.cache.amazonaws.com
#export redisnode6=pywren-redis-10g.oapxhs.clustercfg.usw2.cache.amazonaws.com
#export allnodes=${redisnode1}\;${redisnode2}\;${redisnode3}\;${redisnode4}\;${redisnode5}\;${redisnode6}
export allnodes=${redisnode1}\;${redisnode2}\;${redisnode3}

python /home/ec2-user/pywren/local-clean-redis.py $redisnode1 
python /home/ec2-user/pywren/local-clean-redis.py $redisnode2
python /home/ec2-user/pywren/local-clean-redis.py $redisnode3
#python /home/ec2-user/pywren/local-clean-redis.py $redisnode4
#python /home/ec2-user/pywren/local-clean-redis.py $redisnode5
#python /home/ec2-user/pywren/local-clean-redis.py $redisnode6

#time python pywren-sort-s3.py 2500 1 2500 ${redisnode1}\;${redisnode2} 1000
#time python pywren-part-s3.py 2500 4 2500 4 ${allnodes} 2500
#python /home/ec2-user/pywren/local-clean-redis.py pywren-redis-10b.oapxhs.clustercfg.usw2.cache.amazonaws.com
#time python pywren-part-s3.py 2500 4 2500 4 pywren-redis-10b.oapxhs.clustercfg.usw2.cache.amazonaws.com 100

