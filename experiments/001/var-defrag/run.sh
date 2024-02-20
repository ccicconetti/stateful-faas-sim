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
policies="stateful-best-fit"
defragmentation_intervals="1 5 10 15 30 60 120 300 3600 86400"
state_muls="100 1000 10000"
state_muls="10000"

for state_mul in $state_muls ; do
for defragmentation_interval in $defragmentation_intervals ; do
for policy in $policies ; do
	cmd="./stateful_faas_sim \
		--duration 86400 \
		--job-lifetime 60 \
		--job-interarrival 1 \
		--node-capacity 1000 \
		--defragmentation-interval $defragmentation_interval \
		--state-mul $state_mul \
		--arg-mul 100 \
		--seed-init $SEED_INIT \
		--seed-end $SEED_END \
		--concurrency $CONCURRENCY \
		--policy $policy \
		--output output.csv \
		--append \
		--additional-fields $policy,$state_mul,$defragmentation_interval, \
		--additional-header policy,state-mul,defragmentation-interval,"
	if [ "$DRY_RUN" == "" ] ; then
		echo "policy $policy, state-mul $state_mul, defragmentation-interval $defragmentation_interval"
		eval $cmd
	else
		echo $cmd
	fi
done
done
done
