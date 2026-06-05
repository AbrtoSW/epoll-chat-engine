#!/usr/bin/env python3
import argparse
import asyncio
import random
import struct
import time

SAMPLE_PHRASES = [
    "hey everyone!",
    "how's it going?",
    "any plans for today?",
    "i liked that movie",
    "who's up for coffee?",
    "did you see the update?",
    "lol that was funny",
    "i'm debugging rn",
    "nice to meet you all",
    "see you later"
]

def framed_payload(payload: bytes) -> bytes:
    return struct.pack("!I", len(payload)) + payload

async def run_client(client_id: int,
                     host: str,
                     port: int,
                     messages: int,
                     min_delay: float,
                     max_delay: float,
                     hold_after: float,
                     seed: int):
    rnd = random.Random(seed)
    try:
        reader, writer = await asyncio.open_connection(host, port)
    except Exception as exc:
        print(f"client-{client_id} connect failed: {exc}")
        return

    try:
        for i in range(1, messages + 1):
            text = rnd.choice(SAMPLE_PHRASES)
            payload = f"client-{client_id} msg-{i}: {text}".encode("utf-8")
            writer.write(framed_payload(payload))
            await writer.drain()

            delay = rnd.uniform(min_delay, max_delay)
            await asyncio.sleep(delay)

        if hold_after > 0:
            await asyncio.sleep(hold_after)
    except Exception as exc:
        print(f"client-{client_id} error: {exc}")
    finally:
        writer.close()
        await writer.wait_closed()

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--clients", type=int, default=100)
    parser.add_argument("--messages", type=int, default=5)
    parser.add_argument("--min-delay", type=float, default=0.3)
    parser.add_argument("--max-delay", type=float, default=1.2)
    parser.add_argument("--spawn-delay", type=float, default=0.01)
    parser.add_argument("--hold", type=float, default=0.5)
    parser.add_argument("--batch-size", type=int, default=10000,
                        help="how many clients to create before waiting for them")
    args = parser.parse_args()

    tasks = []
    for client_id in range(1, args.clients + 1):
        seed = int(time.time() * 1000) ^ client_id
        tasks.append(asyncio.create_task(
            run_client(client_id, args.host, args.port,
                       args.messages, args.min_delay, args.max_delay,
                       args.hold, seed)
        ))

        if args.spawn_delay > 0:
            await asyncio.sleep(args.spawn_delay)

        if len(tasks) >= args.batch_size:
            await asyncio.gather(*tasks)
            tasks.clear()

    if tasks:
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())