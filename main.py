"""Root entry point — runs the worker's async loop from app/main.py."""
import asyncio

from app.main import run

if __name__ == "__main__":
    asyncio.run(run())