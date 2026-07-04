"""Simple JSONL target: reads Singer messages from stdin, writes RECORDs to .jsonl files."""
import sys, json, os

output_dir = os.path.join(os.path.dirname(__file__), 'output')
os.makedirs(output_dir, exist_ok=True)

files = {}
schemas = {}

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    msg = json.loads(line)
    msg_type = msg.get('type')

    if msg_type == 'SCHEMA':
        stream = msg['stream']
        schemas[stream] = msg['schema']
        # Write schema file
        schema_path = os.path.join(output_dir, f'{stream}.schema.json')
        with open(schema_path, 'w') as f:
            json.dump(msg, f, indent=2)
        print(f'Schema: {stream}', file=sys.stderr)

    elif msg_type == 'RECORD':
        stream = msg['stream']
        if stream not in files:
            path = os.path.join(output_dir, f'{stream}.jsonl')
            files[stream] = open(path, 'w')
        files[stream].write(json.dumps(msg['record']) + '\n')

    elif msg_type == 'STATE':
        state_path = os.path.join(output_dir, 'state.json')
        with open(state_path, 'w') as f:
            json.dump(msg['value'], f, indent=2)

for f in files.values():
    f.close()

print('Done!', file=sys.stderr)
for stream, fh in files.items():
    path = os.path.join(output_dir, f'{stream}.jsonl')
    size = os.path.getsize(path)
    lines = sum(1 for _ in open(path))
    print(f'  {stream}: {lines} records, {size} bytes', file=sys.stderr)