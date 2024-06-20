import asyncio
import hashlib
from typing import Dict, Any
import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
from datomic import Client

app = FastAPI()

# Global variables
PASTEL_CLI_ENDPOINT = "http://localhost:19932"  # Adjust as needed
DATOMIC_PORT = 4334
datomic_client = None
current_transactor_ip = None

class DataSubmission(BaseModel):
    entity: str
    attribute: str
    value: Any

class Query(BaseModel):
    query: str

async def get_pastel_info():
    async with httpx.AsyncClient() as client:
        best_block_hash = await client.get(f"{PASTEL_CLI_ENDPOINT}/getbestblockhash")
        block = await client.get(f"{PASTEL_CLI_ENDPOINT}/getblock", params={"blockhash": best_block_hash.text})
        pastel_id = await client.get(f"{PASTEL_CLI_ENDPOINT}/pastelid", params={"method": "list"})
        masternode_list = await client.get(f"{PASTEL_CLI_ENDPOINT}/masternodelist", params={"mode": "full"})

    return {
        "merkle_root": block.json()["merkleroot"],
        "pastel_id": pastel_id.json(),
        "masternode_list": masternode_list.json()
    }

def calculate_xor_distance(hash1: str, hash2: str) -> int:
    return int(hash1, 16) ^ int(hash2, 16)

async def determine_current_transactor():
    global current_transactor_ip

    pastel_info = await get_pastel_info()
    merkle_root_hash = hashlib.sha256(pastel_info["merkle_root"].encode()).hexdigest()
    pastel_id_hash = hashlib.sha256(list(pastel_info["pastel_id"].keys())[0].encode()).hexdigest()
    
    own_xor_distance = calculate_xor_distance(merkle_root_hash, pastel_id_hash)
    current_closest = own_xor_distance
    current_transactor_ip = "localhost"  # Default to self

    for node, info in pastel_info["masternode_list"].items():
        if "ENABLED" not in info:
            continue
        node_hash = hashlib.sha256(node.encode()).hexdigest()
        node_distance = calculate_xor_distance(merkle_root_hash, node_hash)
        if node_distance < current_closest:
            current_closest = node_distance
            current_transactor_ip = info.split(':')[0]

    return current_transactor_ip

async def connect_to_datomic():
    global datomic_client, current_transactor_ip
    transactor_ip = await determine_current_transactor()
    if transactor_ip != current_transactor_ip or datomic_client is None:
        current_transactor_ip = transactor_ip
        datomic_client = Client(f"{current_transactor_ip}:{DATOMIC_PORT}/pastel-network")
    return datomic_client

@app.post("/submit")
async def submit_data(data: DataSubmission):
    client = await connect_to_datomic()
    try:
        tx = client.transact([
            {"db/add": {"entity": data.entity, "attribute": data.attribute, "value": data.value}}
        ])
        return {"status": "success", "transaction": tx}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/query")
async def query_data(query: Query):
    client = await connect_to_datomic()
    try:
        results = client.query(query.query)
        return {"status": "success", "results": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def periodic_connection_check():
    while True:
        await connect_to_datomic()
        await asyncio.sleep(60)  # Check every minute

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(periodic_connection_check())

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)