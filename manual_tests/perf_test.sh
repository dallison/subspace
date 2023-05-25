#!/bin/bash

ipaddr=
subspace=true
localhost=true
net=true

if (( $# >= 1 )); then
  ipaddr=$1
  shift
else
  echo "usage: perf_test ipaddr"
  exit 1
fi

for arg in "$*"; do
  case "$arg" in
  --no-subspace)
    subspace=false
    ;;
  --no-localhost)
    localhost=false
    ;;
  --no-net)
    net=false
    ;;
  esac
done

RUN_FAST=
if [[ $(uname) == "Darwin" ]]; then
  RUN_FAST="taskpolicy -c utility"
fi

function run_subspace() {
  size=$1
  num=$2
  $RUN_FAST bazel-bin/manual_tests/perf_subspace_pub --reliable --num_msgs=$num --slot_size=$size &
  $RUN_FAST bazel-bin/manual_tests/perf_subspace_sub --reliable --num_msgs=$num --slot_size=$size --csv &
  wait
}

function run_tcp() {
  size=$1
  host=$2
  num=$3
  pkill perf_tcp_recv
  pkill perf_tcp_send
  ps -ef | grep perf_tcp_recv
  ps -ef | grep perf_tcp_send
  $RUN_FAST bazel-bin/manual_tests/perf_tcp_recv --num_msgs=$num --hostname=$host --msg_size=$size --csv &
  sleep 1
  $RUN_FAST bazel-bin/manual_tests/perf_tcp_send --num_msgs=$num --hostname=$host --msg_size=$size &
  wait
  sleep 1
}


DIR=${HOME}/Documents
subspace_out=${DIR}/subspace.csv
tcp_localhost_out=${DIR}/localhost.csv
tcp_net_out=${DIR}/net.csv
rm -f $subspace_out $tcp_localhost_out $tcp_net_out

if [[ $subspace == true ]]; then
  echo Subspace
  size=1000
  num=1000000
  max=100000000
  while (( size < $max )); do
    echo $size
    run_subspace $size $num >> $subspace_out
    size=$((size*2))
  done
fi

if [[ $localhost == true ]]; then
  echo TCP localhost
  size=1000
  num=10000
  max=8000000
  while (( size < $max )); do
    echo $size
    run_tcp $size localhost $num>> $tcp_localhost_out
    size=$((size*2))
  done
fi 

if [[ $net == true ]]; then
  echo TCP net
  size=1000
  num=10000
  max=8000000
  while (( size < $max )); do
    echo $size
    run_tcp $size $ipaddr $num>> $tcp_net_out
    size=$((size*2))
  done
fi

