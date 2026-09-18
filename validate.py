#!/usr/bin/env python3
"""
System Validation Script
Run this after starting to verify all components work
"""
import asyncio
import os
import sys
from pathlib import Path

# Add project to path
sys.path.insert(0, str(Path(__file__).parent))

# Load .env
from dotenv import load_dotenv
load_dotenv()

async def validate_redis():
    """Test Redis connection"""
    import redis.asyncio as redis
    client = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"))
    try:
        await client.ping()
        await client.set("test:key", "test:value")
        val = await client.get("test:key")
        await client.delete("test:key")
        await client.close()
        assert val == "test:value"
        return True, "Redis OK"
    except Exception as e:
        return False, f"Redis failed: {e}"


async def validate_postgres():
    """Test PostgreSQL connection"""
    import asyncpg
    try:
        conn = await asyncpg.connect(os.getenv("DATABASE_URL"))
        await conn.execute("SELECT 1")
        await conn.close()
        return True, "PostgreSQL OK"
    except Exception as e:
        return False, f"PostgreSQL failed: {e}"


async def validate_kraken():
    """Test Kraken API connection"""
    from hybrid.kraken_executor import KrakenExecutor, KrakenConfig, KrakenEnvironment
    
    api_key = os.getenv("KRAKEN_API_KEY")
    api_secret = os.getenv("KRAKEN_API_SECRET")
    
    if not api_key or not api_secret:
        return False, "Kraken keys not configured"
    
    try:
        config = KrakenConfig(
            api_key=api_key,
            api_secret=api_secret,
            environment=KrakenEnvironment.SPOT
        )
        executor = KrakenExecutor(config)
        await executor.initialize()
        
        # Test ticker
        ticker = await executor.get_ticker("BTC/USD")
        await executor.close()
        
        if ticker and ticker.last > 0:
            return True, f"Kraken OK - BTC/USD: {ticker.last}"
        return False, "Kraken returned invalid ticker"
    except Exception as e:
        return False, f"Kraken failed: {e}"


async def validate_google_llm():
    """Test Google Gemini API"""
    from tradingagents.llm_clients import create_llm_client
    
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        return False, "Google API key not configured"
    
    try:
        client = create_llm_client(
            provider="google",
            model="gemini-2.5-flash",
            api_key=api_key
        )
        llm = client.get_llm()
        response = await llm.ainvoke("Say 'OK' if you receive this")
        return True, f"Google LLM OK - Response: {response.content[:50]}"
    except Exception as e:
        return False, f"Google LLM failed: {e}"


async def validate_solana():
    """Test Solana RPC connection"""
    from solders.rpc.async_api import AsyncClient
    
    rpc_url = os.getenv("SOLANA_RPC_URL")
    private_key = os.getenv("SOLANA_PRIVATE_KEY")
    
    if not rpc_url:
        return False, "Solana RPC URL not configured"
    
    try:
        client = AsyncClient(rpc_url)
        # Test with a known account
        from solders.pubkey import Pubkey
        result = await client.get_balance(Pubkey.from_string("So11111111111111111111111111111111111111112"))
        await client.close()
        return True, f"Solana RPC OK - Wrapped SOL balance: {result.value / 1e9:.4f} SOL"
    except Exception as e:
        return False, f"Solana RPC failed: {e}"


async def validate_evm():
    """Test EVM RPC connection"""
    from web3 import Web3
    
    rpc_url = os.getenv("BASE_RPC_URL")
    private_key = os.getenv("EVM_PRIVATE_KEY")
    
    if not rpc_url:
        return False, "EVM RPC URL not configured"
    
    try:
        w3 = Web3(Web3.HTTPProvider(rpc_url))
        if not w3.is_connected():
            return False, "EVM RPC not connected"
        
        block = w3.eth.block_number
        if private_key:
            acct = w3.eth.account.from_key(private_key)
            balance = w3.eth.get_balance(acct.address)
            return True, f"EVM RPC OK - Block: {block}, Balance: {Web3.from_wei(balance, 'ether'):.4f} ETH"
        return True, f"EVM RPC OK - Block: {block}"
    except Exception as e:
        return False, f"EVM RPC failed: {e}"


async def validate_tradingagents():
    """Test TradingAgents can import and initialize"""
    try:
        from tradingagents.graph.trading_graph import TradingAgentsGraph
        from tradingagents.default_config import DEFAULT_CONFIG
        
        config = DEFAULT_CONFIG.copy()
        config["llm_provider"] = "google"
        config["deep_think_llm"] = "gemini-2.5-flash"
        config["quick_think_llm"] = "gemini-2.5-flash"
        
        ta = TradingAgentsGraph(debug=False, config=config)
        return True, "TradingAgents OK"
    except Exception as e:
        return False, f"TradingAgents failed: {e}"


async def main():
    print("=" * 60)
    print("SYSTEM VALIDATION")
    print("=" * 60)
    
    tests = [
        ("Redis", validate_redis),
        ("PostgreSQL", validate_postgres),
        ("Kraken API", validate_kraken),
        ("Google LLM", validate_google_llm),
        ("TradingAgents", validate_tradingagents),
        ("Solana RPC", validate_solana),
        ("EVM RPC (Base)", validate_evm),
    ]
    
    results = []
    for name, test_func in tests:
        print(f"\n🔍 Testing {name}...")
        try:
            success, message = await test_func()
            if success:
                print(f"  {GREEN}✅ PASS{NC}: {message}")
            else:
                print(f"  {RED}❌ FAIL{NC}: {message}")
            results.append((name, success, message))
        except Exception as e:
            print(f"  {RED}❌ ERROR{NC}: {e}")
            results.append((name, False, str(e)))
    
    # Summary
    print("\n" + "=" * 60)
    print("VALIDATION SUMMARY")
    print("=" * 60)
    
    passed = sum(1 for _, s, _ in results if s)
    total = len(results)
    
    for name, success, message in results:
        status = f"{GREEN}✅{NC}" if success else f"{RED}❌{NC}"
        print(f"  {status} {name}: {message}")
    
    print(f"\nPassed: {passed}/{total}")
    
    if passed == total:
        print(f"\n{GREEN}🎉 All systems operational!{NC}")
        return 0
    else:
        print(f"\n{RED}⚠️  Some systems need attention{NC}")
        return 1


# Colors for output
GREEN = '\033[0;32m'
RED = '\033[0;31m'
NC = '\033[0m'

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
