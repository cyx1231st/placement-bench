import datetime
import logging
import sqlalchemy as sa
from sqlalchemy import sql

import const
import db
import scheduler_structs

LOG = logging.getLogger('placement')


def select_node_info(compute_name):
    (rp_tbl, agg_tbl, rp_agg_tbl, inv_tbl, alloc_tbl) = db.placement_get_tables()
    cn_tbl = sa.alias(rp_tbl, name='cn')
    ram_filtered = sa.alias(inv_tbl, name='ram_filtered')
    cpu_filtered = sa.alias(inv_tbl, name='cpu_filtered')

    ram_usage = sa.select([alloc_tbl.c.resource_provider_id,
                           sql.func.sum(alloc_tbl.c.used).label('used')])
    ram_usage = ram_usage.where(alloc_tbl.c.resource_class_id == const.RAM_MB)
    ram_usage = ram_usage.group_by(alloc_tbl.c.resource_provider_id)
    ram_usage = sa.alias(ram_usage, name='ram_usage')

    cpu_usage = sa.select([alloc_tbl.c.resource_provider_id,
                           sql.func.sum(alloc_tbl.c.used).label('used')])
    cpu_usage = cpu_usage.where(alloc_tbl.c.resource_class_id == const.VCPU)
    cpu_usage = cpu_usage.group_by(alloc_tbl.c.resource_provider_id)
    cpu_usage = sa.alias(cpu_usage, name='cpu_usage')

    ram_inv_join = sql.join(cn_tbl, ram_filtered,
                            sql.and_(
                                cn_tbl.c.id == ram_filtered.c.resource_provider_id,
                                ram_filtered.c.resource_class_id == const.RAM_MB))
    ram_join = sql.outerjoin(ram_inv_join, ram_usage,
                             ram_filtered.c.resource_provider_id == ram_usage.c.resource_provider_id)
    cpu_inv_join = sql.join(ram_join, cpu_filtered,
                            sql.and_(
                                ram_filtered.c.resource_provider_id == cpu_filtered.c.resource_provider_id,
                                cpu_filtered.c.resource_class_id == const.VCPU))
    cpu_join = sql.outerjoin(cpu_inv_join, cpu_usage,
                             cpu_filtered.c.resource_provider_id == cpu_usage.c.resource_provider_id)

    cols_in_output = [
        cn_tbl.c.id,
        cn_tbl.c.name,
        ram_filtered.c.total.label('ram_total'),
        ram_filtered.c.reserved.label('ram_reserved'),
        ram_filtered.c.min_unit.label('ram_min_unit'),
        ram_filtered.c.max_unit.label('ram_max_unit'),
        ram_filtered.c.allocation_ratio.label('ram_allocation_ratio'),
        ram_usage.c.used.label('ram_used'),
        cpu_filtered.c.total.label('cpu_total'),
        cpu_filtered.c.reserved.label('cpu_reserved'),
        cpu_filtered.c.min_unit.label('cpu_min_unit'),
        cpu_filtered.c.max_unit.label('cpu_max_unit'),
        cpu_filtered.c.allocation_ratio.label('cpu_allocation_ratio'),
        cpu_usage.c.used.label('cpu_used'),
    ]

    select = sa.select(cols_in_output).select_from(cpu_join)
    select = select.where(cn_tbl.c.name == compute_name)
    return select


def check(state, claim):
    if state.ram_available < claim.ask_ram:
        return False
    elif state.cpu_available < claim.ask_cpu:
        return False
    return True


def run(args, compute_name, scheduler_queue_map, compute_queue):
    (rp_tbl, agg_tbl, rp_agg_tbl, inv_tbl, alloc_tbl) = db.placement_get_tables()
    engine = db.placement_get_engine()
    conn = engine.connect()
    # smallest_ram = min(r[const.RAM_MB] for r in const.RESOURCE_TEMPLATES)

    # Prepare resource
    select = select_node_info(compute_name)
    records = conn.execute(select)
    records = [record for record in records]
    if len(records) != 1:
        LOG.info("Error in get state of compute node %s" % compute_name)
    compute_state = scheduler_structs.ComputeState(records[0])
    provider_id = records[0]['id']
    for queue in scheduler_queue_map.values():
        queue.put(compute_state)

    def persist(claim):
        created_on = datetime.datetime.utcnow()
        ins = alloc_tbl.insert().values(resource_provider_id=provider_id,
                                        resource_class_id=const.RAM_MB,
                                        consumer_uuid=claim.uuid,
                                        used=claim.ask_ram,
                                        created_at=created_on)
        conn.execute(ins)
        ins = alloc_tbl.insert().values(resource_provider_id=provider_id,
                                        resource_class_id=const.VCPU,
                                        consumer_uuid=claim.uuid,
                                        used=claim.ask_cpu,
                                        created_at=created_on)
        conn.execute(ins)

    while True:
        try:
            claim = compute_queue.get()
        except Exception:
            LOG.debug("Shutdown compute %s" % compute_name)
            return
        reply = None
        if check(compute_state, claim):
            compute_state.cpu_available -= claim.ask_cpu
            compute_state.ram_available -= claim.ask_ram
            reply = scheduler_structs.ClaimReply(claim, True)
            for scheduler in scheduler_queue_map.values():
                scheduler.put(reply)
            persist(claim)
        else:
            reply = scheduler_structs.ClaimReply(claim, False)
            scheduler_queue_map[reply.scheduler].put(reply)
