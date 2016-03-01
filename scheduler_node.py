import logging
import pdb
import random
import six
import sys
import time

import const
import result
import scheduler_structs

LOG = logging.getLogger('scheduler')

MAX_ATTEMPTS = 5


class ForkedPdb(pdb.Pdb):
    """A Pdb subclass that may be used
    from a forked multiprocessing child

    """
    def interaction(self, *args, **kwargs):
        _stdin = sys.stdin
        try:
            sys.stdin = file('/dev/stdin')
            pdb.Pdb.interaction(self, *args, **kwargs)
        finally:
            sys.stdin = _stdin


def do_filtering(node_states, ram, cpu, args, scheduler_name):
    # TODO(Yingxin): speed up algorithm
    result = []
    for node in node_states:
        if args.partition_strategy == 'modulo':
            if ((node.id % args.workers) != scheduler_name):
                continue
        if node.ram_min_unit > ram or node.ram_max_unit < ram \
                or node.ram_available < ram:
            continue
        if node.cpu_min_unit > cpu or node.cpu_max_unit < cpu \
                or node.cpu_available < cpu:
            continue
        result.append(node)
    return result


def schedule(record, records, compute_queue_map,
             node_states, scheduler_name, res, args):
    global GLOBAL
    res.placement_query_count += 1
    ram = record.ask_ram
    cpu = record.ask_cpu
    id = record.uuid
    # filter
    filtered = do_filtering(node_states.values(), ram, cpu,
                            args, scheduler_name)
    if not filtered:
        # no valid host
        del records[record.uuid]
        res.placement_no_found_provider_count += 1
        return None

    res.placement_found_provider_count += 1
    # select
    if args.placement_strategy == 'pack':
        filtered.sort(key=lambda t: (t.ram_available, t.cpu_available, t.id))
    if args.placement_strategy == 'spread':
        filtered.sort(key=lambda t: (t.ram_available, t.cpu_available, -t.id),
                      reverse=True)
    if args.placement_strategy == 'random':
        selected = random.choice(filtered)
    else:
        selected = filtered[0]

    record.state = selected

    # consume
    selected.ram_available -= ram
    selected.cpu_available -= cpu
    claim = scheduler_structs.Claim(id, ram, cpu,
                                    scheduler_name, selected.name)
    # send
    compute_queue_map[selected.name].put(claim)
    res.messages += 1
    return selected


def process_msg(reply, scheduler_queue, scheduler_name,
                node_states, records, compute_queue_map, res, args):
    global MAX_ATTEMPTS
    res.messages += 1
    if reply.scheduler == scheduler_name:
        if reply.proceed:
            # success
            del records[reply.uuid]
        else:
            # failure
            record = records[reply.uuid]
            record.fail()
            if record.attempts >= MAX_ATTEMPTS:
                del records[reply.uuid]
            else:
                record.attempts += 1
                start_placement_query_time = time.time()
                schedule(record, records, compute_queue_map,
                         node_states, scheduler_name, res, args)
                res.add_placement_query_time(time.time() -
                                             start_placement_query_time)
    else:
        if reply.proceed:
            state = node_states[reply.compute]
            state.ram_available -= reply.ask_ram
            state.cpu_available -= reply.ask_cpu
        else:
            LOG.error("NOT GONNA HAPPEN: failed reply from other "
                      "schedulers %s" % reply)


def process_msgs(scheduler_queue, scheduler_name,
                 node_states, records, compute_queue_map, res, args):
    try:
        while True:
            reply = scheduler_queue.get_nowait()
            process_msg(reply, scheduler_queue, scheduler_name,
                        node_states, records, compute_queue_map, res, args)
    except Exception:
        pass


def finishing(scheduler_queue, scheduler_name,
              node_states, records, compute_queue_map, res, args):
    while True:
        if not records:
            break
        reply = scheduler_queue.get()
        process_msg(reply, scheduler_queue, scheduler_name,
                    node_states, records, compute_queue_map, res, args)


def run(args, scheduler_name, compute_queue_map, scheduler_queue,
        done_event, ready_event, start_event,
        request_queue, result_queue):
    count_computes = len(compute_queue_map)

    node_states = {}
    for _ in six.moves.xrange(count_computes):
        node_state = scheduler_queue.get()
        node_states[node_state.name] = node_state
    
    ready_event.set()
    LOG.info("Scheduler %s is ready!" % scheduler_name)

    start_event.wait()
    res = result.Result()
    attempt_records = {}

    while True:
        entry = request_queue.get()
        res.messages += 1
        if entry is None:
            finishing(scheduler_queue, scheduler_name, node_states,
                      attempt_records, compute_queue_map, res, args)
            LOG.info("No more entries in request queue after processing "
                     "%d requests. Sending results."
                     % res.requests_processed_count)
            break

        res.requests_processed_count += 1

        process_msgs(scheduler_queue, scheduler_name, node_states,
                     attempt_records, compute_queue_map, res, args)

        instance_uuid = entry[0]
        res_template = entry[1]
        ask_ram = res_template[const.RAM_MB]
        ask_cpu = res_template[const.VCPU]

        if instance_uuid in attempt_records:
            # Cannot happen
            LOG.error("Request id %s already in records of scheduler %s"
                      % (instance_uuid, scheduler_name))
            break
        else:
            record = scheduler_structs.Record(instance_uuid, ask_ram, ask_cpu)
            attempt_records[instance_uuid] = record
            start_placement_query_time = time.time()
            schedule(record, attempt_records, compute_queue_map,
                     node_states, scheduler_name, res, args)
            res.add_placement_query_time(time.time() -
                                         start_placement_query_time)

    result_queue.put(res)
    done_event.set()
