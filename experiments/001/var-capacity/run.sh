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
node_capacities="800 1600 2400 3200 4000"
defragmentation_intervals="30 120"

for node_capacity in $node_capacities ; do
for defragmentation_interval in $defragmentation_intervals ; do
for policy in $policies ; do
	if [[ "$policy" != "stateful-best-fit" && "$defragmentation_interval" != "30" ]] ; then
		continue
	fi

	cmd="./stateful_faas_sim \
		--duration 86400 \
		--job-lifetime 120 \
		--job-interarrival 1 \
		--job-invocation-rate 5 \
		--node-capacity $node_capacity \
		--defragmentation-interval $defragmentation_interval \
		--state-mul 10000 \
		--arg-mul 100 \
		--seed-init $SEED_INIT \
		--seed-end $SEED_END \
		--concurrency $CONCURRENCY \
		--policy $policy \
		--output output.csv \
		--append \
		--additional-fields $policy,$node_capacity,$defragmentation_interval, \
		--additional-header policy,node-capacity,defragmentation-interval,"
	if [ "$DRY_RUN" == "" ] ; then
		echo "policy $policy, node-capacity $node_capacity, defragmentation-interval $defragmentation_interval"
		eval $cmd
	else
		echo $cmd
	fi
done
done
done
