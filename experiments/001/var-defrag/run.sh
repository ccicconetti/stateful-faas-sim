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
defragmentation_intervals="15 30 60 120 300 3600"

for defragmentation_interval in $defragmentation_intervals ; do
for policy in $policies ; do
	cmd="./stateful_faas_sim \
		--duration 86400 \
		--job-lifetime 60 \
		--job-interarrival 1 \
		--node-capacity 1000 \
		--defragmentation-interval $defragmentation_interval \
		--state-mul 100 \
		--arg-mul 100 \
		--seed-init $SEED_INIT \
		--seed-end $SEED_END \
		--concurrency $CONCURRENCY \
		--policy $policy \
		--output output.csv \
		--append \
		--additional-fields $policy,$defragmentation_interval, \
		--additional-header policy,defragmentation-interval,"
	if [ "$DRY_RUN" == "" ] ; then
		echo "policy $policy, defragmentation-interval $defragmentation_interval"
		eval $cmd
	else
		echo $cmd
	fi
done
done
