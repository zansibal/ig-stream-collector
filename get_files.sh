dir=$(cd -P -- "$(dirname -- "$0")" && pwd -P)
KEY="aws/LightsailDefaultKey-eu-west-1.pem"
IP=`cat aws/ip`
YEAR=$(date +%Y)
WEEK=$(date +%U) # This is the week number with Sunday as start of week
#WEEK=12 # DEBUG

for d in ~/data/indy/prices/ig_streaming/tick/*/; do
	epic="$(basename $d)" 
	exec scp -P 22 \
		 -o "ConnectTimeout 3" \
		 -o "StrictHostKeyChecking no" \
		 -o "UserKnownHostsFile /dev/null" \
		 -i $KEY \
		 ec2-user@$IP:data/tick_weekly/$epic/"$epic"_$YEAR-$WEEK.ftr $d
	#break # DEBUG
done
