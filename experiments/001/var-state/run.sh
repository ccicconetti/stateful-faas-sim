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
state_muls="100 200 500 1000 2000 5000 10000 20000 50000"

for state_mul in $state_muls ; do
for policy in $policies ; do
	cmd="./stateful_faas_sim \
		--duration 86400 \
		--job-lifetime 60 \
		--job-interarrival 1 \
		--job-invocation-rate 5 \
		--node-capacity 1000 \
		--defragmentation-interval 120 \
		--state-mul $state_mul \
		--arg-mul 100 \
		--seed-init $SEED_INIT \
		--seed-end $SEED_END \
		--concurrency $CONCURRENCY \
		--policy $policy \
		--output output.csv \
		--append \
		--additional-fields $policy,$state_mul, \
		--additional-header policy,state-mul,"
	if [ "$DRY_RUN" == "" ] ; then
		echo "policy $policy"
		eval $cmd
	else
		echo $cmd
	fi
done
done
