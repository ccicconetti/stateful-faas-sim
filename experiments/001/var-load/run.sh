#!/bin/bash

if [ "$CONCURRENCY" == "" ] ; then
	CONCURRENCY=10
fi
if [ "$SEED_INIT" == "" ] ; then
	SEED_INIT=0
fi
if [ "$SEED_END" == "" ] ; then
	SEED_END=10
fi

if [ ! -x ./stateful_faas_sim ] ; then
	echo "the executable 'stateful_faas_sim' is missing"
	exit 1
fi

if [ ! -d ./data ] ; then
	echo "the data directory is missing"
	exit 1
fi

policies="stateless-min-nodes stateless-max-balancing stateful-best-fit stateful-random"
job_lifetimes="15 30 45 60 75 90 105 120"

for job_lifetime in $job_lifetimes ; do
for policy in $policies ; do
	cmd="./stateful_faas_sim \
		--duration 86400 \
		--job-lifetime $job_lifetime \
		--job-interarrival 1 \
		--job-invocation-rate 5 \
		--node-capacity 1000 \
		--defragmentation-interval 120 \
		--state-mul 10000 \
		--arg-mul 100 \
		--seed-init $SEED_INIT \
		--seed-end $SEED_END \
		--concurrency $CONCURRENCY \
		--policy $policy \
		--output output.csv \
		--append \
		--additional-fields $policy,$job_lifetime, \
		--additional-header policy,job-lifetime,"
	if [ "$DRY_RUN" == "" ] ; then
		echo "policy $policy, job-lifetime $job_lifetime"
		eval $cmd
	else
		echo $cmd
	fi
done
done
