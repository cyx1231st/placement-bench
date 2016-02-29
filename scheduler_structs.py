import time


class ComputeState(object):
    __slots__ = ['id',
                 'name',
                 'ram_available',
                 'ram_min_unit',
                 'ram_max_unit',
                 'cpu_available',
                 'cpu_min_unit',
                 'cpu_max_unit']
    def __init__(self, record):
        self.id = record['id']
        self.name = record['name']
        self.ram_available = (record['ram_total'] - record['ram_reserved']) * \
                record['ram_allocation_ratio']
        self.ram_min_unit = record['ram_min_unit']
        self.ram_max_unit = record['ram_max_unit']
        self.cpu_available = (record['cpu_total'] - record['cpu_reserved']) * \
                record['cpu_allocation_ratio']
        self.cpu_min_unit = record['cpu_min_unit']
        self.cpu_max_unit = record['cpu_max_unit']

    def __repr__(self):
        return "compute %s: ram(%s), cpu(%s)" \
               % (self.name, self.ram_available, self.cpu_available)

class Record(object):
    __slots__ = ['uuid',
                 'ask_ram',
                 'ask_cpu',
                 'attempts',
                 'start_at',
                 'state']

    def __init__(self, id, ram, cpu):
        self.uuid = id
        self.ask_ram = ram
        self.ask_cpu = cpu
        self.attempts = 1
        self.start_at = time.time()
        self.state = None

    def fail(self):
        self.state.ram_available += self.ask_ram
        self.state.cpu_available += self.ask_cpu

class Claim(object):
    __slots__ = ['uuid',
                 'ask_ram',
                 'ask_cpu',
                 'scheduler',
                 'compute']

    def __init__(self, id, ram, cpu, scheduler, compute):
        self.uuid = id
        self.ask_ram = ram
        self.ask_cpu = cpu
        self.scheduler = scheduler
        self.compute = compute

class ClaimReply(object):
    __slots__ = ['uuid',
                 'proceed',
                 'compute',
                 'scheduler',
                 'ask_cpu',
                 'ask_ram']

    def __init__(self, claim, proceed):
        self.uuid = claim.uuid
        self.proceed = proceed
        self.compute = claim.compute
        self.scheduler = claim.scheduler
        self.ask_cpu = claim.ask_cpu
        self.ask_ram = claim.ask_ram
