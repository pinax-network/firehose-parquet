#!/usr/bin/env python3
from google.protobuf import descriptor_pb2,descriptor_pool,message_factory
from pathlib import Path
import hashlib,json,subprocess,struct,decimal
import argparse
parser=argparse.ArgumentParser(description='Compare Bitcoin block 900000 raw protobuf and serialized outputs with a local Parquet capture; no network calls.')
parser.add_argument('--raw-dir',type=Path,required=True,help='Directory containing block.pb and public metadata.json')
parser.add_argument('--descriptor',type=Path,required=True,help='protoc --include_imports descriptor for proto/bitcoin.proto')
parser.add_argument('--dataset',type=Path,required=True,help='Local chain root containing blocks/, transactions/, inputs/, outputs/')
parser.add_argument('--cursor',type=Path,required=True,help='Local cursor.parquet; reads only last_block_num')
parser.add_argument('--duckdb',default='duckdb')
args=parser.parse_args()
root=args.raw_dir
dataset=str(args.dataset.resolve()).replace("'","''")
cursor_path=str(args.cursor.resolve()).replace("'","''")
fds=descriptor_pb2.FileDescriptorSet.FromString(args.descriptor.read_bytes());pool=descriptor_pool.DescriptorPool()
for file in fds.file:pool.Add(file)
Block=message_factory.GetMessageClass(pool.FindMessageTypeByName('sf.bitcoin.type.v1.Block'))
data=root.joinpath('block.pb').read_bytes();b=Block.FromString(data)
meta=json.loads(root.joinpath('metadata.json').read_text());assert hashlib.sha256(data).hexdigest()==meta['sha256']
assert b.height==meta['block_num']==900000 and b.hash==meta['block_id'] and b.previous_hash==meta['parent_id'] and b.time==meta['timestamp']
def sql(q):return json.loads(subprocess.check_output([args.duckdb,'-json','-c',q],text=True))
def table(name):
    columns = '* EXCLUDE(witness), to_json(witness) AS witness' if name=='inputs' else '*'
    return sql(f"SELECT {columns} FROM read_parquet('{dataset}/{name}/**/*.parquet')")

def integer(x):return int(x)
def eq(a,e,where):assert a==e,(where,a,e)
# Parse raw transaction prefix independently with BytesIO + struct, extracting
# integer output units directly rather than trusting a rounded protobuf double.
import io
def units(tx):
    if not tx.hex:
        raise AssertionError('live qualification requires serialized transactions')
    r=io.BytesIO(bytes.fromhex(tx.hex));assert len(r.read(4))==4
    def size():
        first=r.read(1)[0]
        return first if first<253 else struct.unpack({253:'<H',254:'<I',255:'<Q'}[first],r.read({253:2,254:4,255:8}[first]))[0]
    first=r.read(1)[0]
    if first==0:
        assert r.read(1)[0] !=0
    else:r.seek(-1,1)
    for _ in range(size()):
        assert len(r.read(36))==36
        length=size();assert len(r.read(length))==length
        assert len(r.read(4))==4
    values=[]
    for _ in range(size()):
        value=struct.unpack('<q',r.read(8))[0];assert value>=0;values.append(value)
        length=size();assert len(r.read(length))==length
    return values
blocks=table('blocks');transactions=table('transactions');inputs=table('inputs');outputs=table('outputs')
eq(len(blocks),1,'blocks');eq(len(transactions),len(b.tx),'transactions');eq(len(inputs),sum(len(t.vin) for t in b.tx),'inputs');eq(len(outputs),sum(len(t.vout) for t in b.tx),'outputs')
for name,rows in [('blocks',blocks),('transactions',transactions),('inputs',inputs),('outputs',outputs)]:
    for row in rows:
        eq(integer(row['block_num']),900000,(name,'block_num'));eq(row['block_id'],'0x'+b.hash,(name,'block_id'));eq(row['parent_id'],'0x'+b.previous_hash,(name,'parent_id'))
eq(blocks[0]['hash'],b.hash,'native block hash')
transactions.sort(key=lambda r:integer(r['tx_index']));inputs.sort(key=lambda r:(integer(r['tx_index']),integer(r['input_index'])))
output_lookup={(r['tx_hash'],integer(r['output_index'])):r for r in outputs};assert len(output_lookup)==len(outputs)
inpos=0;exact_total=0;fallback_addresses=0;null_addresses=0;witness_inputs=0;empty_present_scripts=0
for tx_index,t in enumerate(b.tx):
    tr=transactions[tx_index];eq(tr['txid'],t.txid,('txid',tx_index));eq(tr['hash'],t.hash,('hash',tx_index))
    raw_units=units(t);eq(len(raw_units),len(t.vout),('raw output count',tx_index))
    for j,vin in enumerate(t.vin):
        row=inputs[inpos];inpos+=1
        eq(integer(row['tx_index']),tx_index,'input tx_index');eq(integer(row['input_index']),j,'input_index');eq(row['tx_hash'],t.txid,'input txhash')
        coinbase=vin.coinbase or None;prev=None if coinbase else (vin.txid or None)
        eq(row['coinbase'],coinbase,'coinbase');eq(row['prev_txid'],prev,'prev_txid');eq(None if row['prev_vout'] is None else integer(row['prev_vout']),None if prev is None else vin.vout,'prev_vout')
        script=vin.script_sig if vin.HasField('script_sig') and not coinbase else None
        eq(row['script_sig_asm'],None if script is None else script.asm,'scriptasm');eq(row['script_sig_hex'],None if script is None else script.hex,'scripthex');eq(row['witness'],list(vin.txinwitness),'witness')
        witness_inputs+=bool(vin.txinwitness);empty_present_scripts+=script is not None and not script.hex
    for j,vout in enumerate(t.vout):
        row=output_lookup[(t.txid,vout.n)];eq(vout.n,j,'output n');eq(integer(row['value_sats']),raw_units[j],('satoshis',tx_index,j));eq(row['value'],vout.value,'legacyfloat')
        # Decimal conversion of JSON-like decimal representation is independent
        # of the production float-neighbor fallback and serialized decoder.
        eq(int(decimal.Decimal(str(vout.value))*100_000_000),raw_units[j],'decimal units')
        exact_total+=raw_units[j]
        script=vout.script_pubKey if vout.HasField('script_pubKey') else None
        address=None if script is None else script.address or (script.addresses[0] if script.addresses else None) or None
        fallback_addresses+=bool(script is not None and not script.address and script.addresses);null_addresses+=address is None
        eq(row['script_pubkey_address'],address,'address')
        for column,field in [('script_pubkey_asm','asm'),('script_pubkey_hex','hex'),('script_pubkey_type','type')]:eq(row[column],None if script is None else getattr(script,field),column)
eq(integer(sql(f"SELECT SUM(value_sats) AS total FROM read_parquet('{dataset}/outputs/**/*.parquet')")[0]['total']),exact_total,'SQL integer sum')
# Report only public cursor progress, never opaque cursor content.
cursor=sql(f"SELECT last_block_num FROM read_parquet('{cursor_path}')")[0]
eq(integer(cursor['last_block_num']),900000,'cursor frontier')
result={'block':900000,'raw_sha256':meta['sha256'],'counts':{'blocks':len(blocks),'transactions':len(transactions),'inputs':len(inputs),'outputs':len(outputs)},'total_satoshis':str(exact_total),'witness_inputs':witness_inputs,'present_empty_scripts':empty_present_scripts,'legacy_address_fallbacks':fallback_addresses,'null_addresses':null_addresses,'cursor_last_block':900000,'parts':len(list(args.dataset.rglob('*.parquet'))),'hidden_temporary_files':len(list(args.dataset.rglob('.*.tmp')))}
print(json.dumps(result,indent=2))
