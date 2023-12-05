# pylint: skip-file
import json
import math

loaders = 6
multiplier = 1
count = loaders*multiplier
num_of_rows = 650000000
gauss_ratio = 99.9  # higher value increases cache hit ratio
rows_per_command = math.ceil(num_of_rows/count)
prepare_write_cs = f"cassandra-stress write no-warmup cl=ALL n={rows_per_command} -schema 'replication(strategy=NetworkTopologyStrategy,replication_factor=3)' -mode cql3 native -rate threads=200 -col 'size=FIXED(128) n=FIXED(8)' "
write_cs = "cassandra-stress write no-warmup cl=QUORUM duration=60m -schema 'replication(strategy=NetworkTopologyStrategy,replication_factor=3)' -mode cql3 native -rate threads=340 -col 'size=FIXED(128) n=FIXED(8)'"
read_cs = "cassandra-stress read no-warmup cl=QUORUM duration=60m -mode cql3 native -rate threads=380 -col 'size=FIXED(128) n=FIXED(8)'"
mixed_cs = "cassandra-stress mixed no-warmup cl=QUORUM duration=60m -mode cql3 native -rate threads=350 -col 'size=FIXED(128) n=FIXED(8)'"

commands = []
for idx in range(count):
    rows = idx*rows_per_command
    commands.append(f"{prepare_write_cs} -pop seq={rows + 1:.0f}..{rows + rows_per_command:.0f}")
print("prepare write:")
print(json.dumps(commands, indent=2))


commands = []
for idx in range(count):
    rows = idx*rows_per_command
    commands.append(f"{write_cs} -pop seq={rows + 1:.0f}..{rows + rows_per_command:.0f}")
print("Write:")
print(json.dumps(commands, indent=2))

commands = []
for idx in range(count):
    min_row = idx * rows_per_command + 1
    max_row = idx * rows_per_command + rows_per_command
    mean = (min_row + max_row) / 2
    commands.append(
        f"{read_cs} -pop 'dist=gauss({min_row:.0f}..{max_row:.0f},{mean:.0f},{(mean - min_row) / gauss_ratio:.0f})'")
print("Read:")
print(json.dumps(commands, indent=2))

commands = []
for idx in range(count):
    min_row = idx * rows_per_command + 1
    max_row = idx * rows_per_command + rows_per_command
    mean = (min_row + max_row) / 2
    commands.append(
        f"{mixed_cs} -pop 'dist=gauss({min_row:.0f}..{max_row:.0f},{mean:.0f},{(mean - min_row) / gauss_ratio:.0f})'")

print("Mixed:")
print(json.dumps(commands, indent=2))
