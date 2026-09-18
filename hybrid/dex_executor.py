"""
DEX Executor
============
Multi-chain DEX execution layer for the sniper bot.
Supports Jupiter (Solana), Uniswap (EVM), and extensible for others.
"""

import asyncio
import json
import os
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional, Dict, List, Any
from enum import Enum

import aiohttp
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.transaction import VersionedTransaction
from solders.message import to_bytes_versioned
import base58
import base64

# EVM imports (optional)
try:
    from web3 import Web3
    from web3.middleware import geth_poa_middleware
    from eth_account import Account
    EVM_AVAILABLE = True
except ImportError:
    EVM_AVAILABLE = False


class Chain(Enum):
    SOLANA = "solana"
    ETHEREUM = "ethereum"
    BASE = "base"
    ARBITRUM = "arbitrum"
    POLYGON = "polygon"
    BSC = "bsc"


class DexProtocol(Enum):
    JUPITER = "jupiter"
    RAYDIUM = "raydium"
    ORCA = "orca"
    UNISWAP_V3 = "uniswap_v3"
    UNISWAP_V4 = "uniswap_v4"
    PANCAKESWAP = "pancakeswap"
    SUSHISWAP = "sushiswap"


@dataclass
class QuoteRequest:
    input_mint: str      # Token to sell (e.g., USDC mint)
    output_mint: str     # Token to buy
    amount: int          # Amount in smallest units (lamports/wei)
    slippage_bps: int = 100  # 1% default


@dataclass
class QuoteResponse:
    input_amount: int
    output_amount: int
    price_impact_pct: float
    route: List[Dict]
    dex: DexProtocol
    valid_for_seconds: int = 30


@dataclass
class SwapResult:
    success: bool
    signature: str
    input_amount: int
    output_amount: int
    fee_paid: int
    price_impact_pct: float
    error: Optional[str] = None


class DexExecutor(ABC):
    """Abstract base for DEX executors"""
    
    def __init__(self, chain: Chain, rpc_url: str, private_key: str):
        self.chain = chain
        self.rpc_url = rpc_url
        self.private_key = private_key
        self.session: Optional[aiohttp.ClientSession] = None
    
    @abstractmethod
    async def get_quote(self, request: QuoteRequest) -> QuoteResponse: ...
    
    @abstractmethod
    async def execute_swap(self, quote: QuoteResponse) -> SwapResult: ...
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()


# =============================================================================
# JUPITER (SOLANA) EXECUTOR
# =============================================================================

JUPITER_QUOTE_API = "https://lite-api.jup.ag/swap/v1/quote"  # ponytail: hard-coded URL, config-ify if Jupiter changes again
JUPITER_SWAP_API = "https://lite-api.jup.ag/swap/v1/swap"  # ponytail: hard-coded URL, config-ify if Jupiter changes again
JUPITER_TOKENS_API = "https://token.jup.ag/all"


class JupiterExecutor(DexExecutor):
    """
    Jupiter Aggregator on Solana
    - Best prices across all Solana DEXs (Raydium, Orca, Phoenix, etc.)
    - Supports pump.fun launches via Jupiter's routing
    - Built-in slippage protection, priority fees
    """
    
    def __init__(
        self,
        rpc_url: str = None,
        private_key: str = None,
        priority_fee_lamports: int = 100_000,  # 0.0001 SOL
        compute_unit_limit: int = 200_000,
        compute_unit_price: int = 10_000  # micro-lamports
    ):
        rpc_url = rpc_url or os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
        private_key = private_key or os.getenv("SOLANA_PRIVATE_KEY")
        
        if not private_key:
            raise ValueError("SOLANA_PRIVATE_KEY required")
        
        super().__init__(Chain.SOLANA, rpc_url, private_key)
        
        # Decode private key
        self.keypair = Keypair.from_base58_string(private_key)
        self.pubkey = self.keypair.pubkey()
        
        # Transaction settings
        self.priority_fee_lamports = priority_fee_lamports
        self.compute_unit_limit = compute_unit_limit
        self.compute_unit_price = compute_unit_price
        
        # Token cache
        self._token_cache: Dict[str, Dict] = {}
    
    async def get_quote(self, request: QuoteRequest) -> QuoteResponse:
        """Get best quote from Jupiter"""
        if not self.session:
            raise RuntimeError("Session not initialized. Use async context manager.")
        
        params = {
            "inputMint": request.input_mint,
            "outputMint": request.output_mint,
            "amount": str(request.amount),
            "slippageBps": str(request.slippage_bps),
            "onlyDirectRoutes": "false",
            "asLegacyTransaction": "false",
            "platformFeeBps": "0"
        }
        
        async with self.session.get(JUPITER_QUOTE_API, params=params) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise Exception(f"Jupiter quote failed: {resp.status} - {text}")
            data = await resp.json()
        
        return QuoteResponse(
            input_amount=int(data["inAmount"]),
            output_amount=int(data["outAmount"]),
            price_impact_pct=float(data.get("priceImpactPct", 0)),
            route=data.get("routePlan", []),
            dex=DexProtocol.JUPITER,
            valid_for_seconds=30
        )
    
    async def execute_swap(self, quote: QuoteResponse) -> SwapResult:
        """Execute swap via Jupiter"""
        if not self.session:
            raise RuntimeError("Session not initialized")
        
        # Get swap transaction
        swap_request = {
            "quoteResponse": {
                "inputMint": quote.route[0]["swapInfo"]["inputMint"] if quote.route else "",
                "outputMint": quote.route[0]["swapInfo"]["outputMint"] if quote.route else "",
                "inAmount": str(quote.input_amount),
                "outAmount": str(quote.output_amount),
                "routePlan": quote.route
            },
            "userPublicKey": str(self.pubkey),
            "wrapAndUnwrapSol": True,
            "useSharedAccounts": True,
            "feeAccount": None,
            "computeUnitPriceMicroLamports": self.compute_unit_price,
            "prioritizationFeeLamports": self.priority_fee_lamports,
            "asLegacyTransaction": False,
            "useTokenLedger": False
        }
        
        async with self.session.post(JUPITER_SWAP_API, json=swap_request) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise Exception(f"Jupiter swap failed: {resp.status} - {text}")
            data = await resp.json()
        
        # Deserialize and sign transaction
        swap_tx_b64 = data["swapTransaction"]
        swap_tx_bytes = base64.b64decode(swap_tx_b64)
        versioned_tx = VersionedTransaction.from_bytes(swap_tx_bytes)
        
        # Sign
        message_bytes = to_bytes_versioned(versioned_tx.message)
        signature = self.keypair.sign_message(message_bytes)
        signed_tx = VersionedTransaction(
            versioned_tx.message,
            [signature]
        )
        
        # Send transaction
        signed_b64 = base64.b64encode(bytes(signed_tx)).decode()
        
        rpc_payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendTransaction",
            "params": [
                signed_b64,
                {
                    "encoding": "base64",
                    "skipPreflight": False,
                    "preflightCommitment": "confirmed",
                    "maxRetries": 3
                }
            ]
        }
        
        async with self.session.post(self.rpc_url, json=rpc_payload) as resp:
            result = await resp.json()
        
        if "error" in result:
            return SwapResult(
                success=False,
                signature="",
                input_amount=quote.input_amount,
                output_amount=quote.output_amount,
                fee_paid=0,
                price_impact_pct=quote.price_impact_pct,
                error=result["error"]["message"]
            )
        
        tx_sig = result["result"]
        
        # Wait for confirmation
        confirmed = await self._confirm_transaction(tx_sig)
        
        return SwapResult(
            success=confirmed,
            signature=tx_sig,
            input_amount=quote.input_amount,
            output_amount=quote.output_amount,
            fee_paid=self.priority_fee_lamports,
            price_impact_pct=quote.price_impact_pct,
            error=None if confirmed else "Confirmation timeout"
        )
    
    async def _confirm_transaction(self, signature: str, max_retries: int = 30) -> bool:
        """Wait for transaction confirmation"""
        for _ in range(max_retries):
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getSignatureStatuses",
                "params": [[signature], {"searchTransactionHistory": True}]
            }
            async with self.session.post(self.rpc_url, json=payload) as resp:
                result = await resp.json()
            
            if result.get("result", {}).get("value", [None])[0]:
                status = result["result"]["value"][0]
                if status.get("confirmationStatus") in ["confirmed", "finalized"]:
                    return True
                if status.get("err"):
                    return False
            
            await asyncio.sleep(1)
        
        return False
    
    async def get_token_info(self, mint: str) -> Dict:
        """Get token metadata from Jupiter"""
        if mint in self._token_cache:
            return self._token_cache[mint]
        
        async with self.session.get(JUPITER_TOKENS_API) as resp:
            tokens = await resp.json()
        
        for token in tokens:
            if token["address"] == mint:
                self._token_cache[mint] = token
                return token
        
        return {"address": mint, "symbol": "UNKNOWN", "decimals": 9}
    
    async def get_sol_balance(self) -> int:
        """Get SOL balance in lamports"""
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBalance",
            "params": [str(self.pubkey)]
        }
        async with self.session.post(self.rpc_url, json=payload) as resp:
            result = await resp.json()
        return result["result"]["value"]
    
    async def get_token_balance(self, mint: str) -> int:
        """Get SPL token balance"""
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTokenAccountsByOwner",
            "params": [
                str(self.pubkey),
                {"mint": mint},
                {"encoding": "jsonParsed"}
            ]
        }
        async with self.session.post(self.rpc_url, json=payload) as resp:
            result = await resp.json()
        
        accounts = result.get("result", {}).get("value", [])
        if accounts:
            return int(accounts[0]["account"]["data"]["parsed"]["info"]["tokenAmount"]["amount"])
        return 0


# =============================================================================
# UNISWAP V3 (EVM) EXECUTOR
# =============================================================================

UNISWAP_V3_QUOTER = "0xb27308f9F90D607463bb33eA1BeBb41C27CE5AB6"
UNISWAP_V3_ROUTER = "0xE592427A0AEce92De3Edee1F18E0157C05861564"

# Minimal ABIs
QUOTER_ABI = json.loads("""[
    {"inputs":[{"internalType":"address","name":"tokenIn","type":"address"},
               {"internalType":"address","name":"tokenOut","type":"address"},
               {"internalType":"uint24","name":"fee","type":"uint24"},
               {"internalType":"uint256","name":"amountIn","type":"uint256"},
               {"internalType":"bool","name":"sqrtPriceLimitX96","type":"bool"}],
     "name":"quoteExactInputSingle",
     "outputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"}],
     "stateMutability":"nonpayable","type":"function"}
]""")

ROUTER_ABI = json.loads("""[
    {"inputs":[{"components":[{"internalType":"address","name":"tokenIn","type":"address"},
                               {"internalType":"address","name":"tokenOut","type":"address"},
                               {"internalType":"uint24","name":"fee","type":"uint24"},
                               {"internalType":"address","name":"recipient","type":"address"},
                               {"internalType":"uint256","name":"deadline","type":"uint256"},
                               {"internalType":"uint256","name":"amountIn","type":"uint256"},
                               {"internalType":"uint256","name":"amountOutMinimum","type":"uint256"},
                               {"internalType":"uint160","name":"sqrtPriceLimitX96","type":"uint160"}],
              "internalType":"struct ISwapRouter.ExactInputSingleParams",
              "name":"params","type":"tuple"}],
     "name":"exactInputSingle",
     "outputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"}],
     "stateMutability":"payable","type":"function"}
]""")

ERC20_ABI = json.loads("""[
    {"constant":true,"inputs":[{"name":"_owner","type":"address"}],
     "name":"balanceOf","outputs":[{"name":"balance","type":"uint256"}],"type":"function"},
    {"constant":false,"inputs":[{"name":"_spender","type":"address"},{"name":"_value","type":"uint256"}],
     "name":"approve","outputs":[{"name":"","type":"bool"}],"type":"function"},
    {"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"type":"function"},
    {"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"type":"function"}
]""")


class UniswapV3Executor(DexExecutor):
    """Uniswap V3 on EVM chains (Ethereum, Base, Arbitrum, Polygon, BSC)"""
    
    # Chain configs
    CHAIN_CONFIG = {
        Chain.ETHEREUM: {
            "rpc": "https://eth.llamarpc.com",
            "quoter": "0xb27308f9F90D607463bb33eA1BeBb41C27CE5AB6",
            "router": "0xE592427A0AEce92De3Edee1F18E0157C05861564",
            "chain_id": 1,
            "native": "ETH",
            "wrapped_native": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
            "usdc": "0xA0b86a33E6441b8c4C8C8C8C8C8C8C8C8C8C8C8C8"  # placeholder
        },
        Chain.BASE: {
            "rpc": "https://mainnet.base.org",
            "quoter": "0x3d4e44Eb1374240CE5F1B871ab261CD16335B76a",
            "router": "0x2626664c2603336E57B271c5C0b26F421741e481",
            "chain_id": 8453,
            "native": "ETH",
            "wrapped_native": "0x4200000000000000000000000000000000000006",
            "usdc": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
        },
        Chain.ARBITRUM: {
            "rpc": "https://arb1.arbitrum.io/rpc",
            "quoter": "0x61fFE014bA17989E743c5F6cB21bF9697530B21e",
            "router": "0xE592427A0AEce92De3Edee1F18E0157C05861564",
            "chain_id": 42161,
            "native": "ETH",
            "wrapped_native": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            "usdc": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        }
    }
    
    # Common fee tiers
    FEE_TIERS = [100, 500, 3000, 10000]  # 0.01%, 0.05%, 0.3%, 1%
    
    def __init__(
        self,
        chain: Chain = Chain.BASE,
        rpc_url: str = None,
        private_key: str = None,
        gas_multiplier: float = 1.2
    ):
        if not EVM_AVAILABLE:
            raise ImportError("web3 and eth_account required: pip install web3 eth-account")
        
        config = self.CHAIN_CONFIG.get(chain)
        if not config:
            raise ValueError(f"Unsupported chain: {chain}")
        
        rpc_url = rpc_url or config["rpc"]
        private_key = private_key or os.getenv(f"{chain.value.upper()}_PRIVATE_KEY") or os.getenv("EVM_PRIVATE_KEY")
        
        if not private_key:
            raise ValueError(f"Private key required for {chain.value}")
        
        super().__init__(chain, rpc_url, private_key)
        
        self.config = config
        self.gas_multiplier = gas_multiplier
        
        # Setup web3
        self.w3 = Web3(Web3.HTTPProvider(rpc_url))
        if chain in [Chain.BSC, Chain.POLYGON]:
            self.w3.middleware_onion.inject(geth_poa_middleware, layer=0)
        
        self.account = Account.from_key(private_key)
        self.address = self.account.address
        
        # Contracts
        self.quoter = self.w3.eth.contract(
            address=Web3.to_checksum_address(config["quoter"]),
            abi=QUOTER_ABI
        )
        self.router = self.w3.eth.contract(
            address=Web3.to_checksum_address(config["router"]),
            abi=ROUTER_ABI
        )
        
        # Wrapped native token
        self.wrapped_native = Web3.to_checksum_address(config["wrapped_native"])
    
    def _resolve_token(self, token: str) -> str:
        """Resolve token symbol to address"""
        # Check if already address
        if token.startswith("0x") and len(token) == 42:
            return Web3.to_checksum_address(token)
        
        # Known tokens per chain
        known = {
            Chain.BASE: {
                "USDC": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
                "WETH": "0x4200000000000000000000000000000000000006",
                "DAI": "0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb",
            },
            Chain.ETHEREUM: {
                "USDC": "0xA0b86a33E6441b8c4C8C8C8C8C8C8C8C8C8C8C8C8",
                "WETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
                "USDT": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
                "DAI": "0x6B175474E89094C44Da98b954EedeAC495271d0F",
            },
            Chain.ARBITRUM: {
                "USDC": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                "WETH": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
                "USDT": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
            }
        }
        
        chain_known = known.get(self.chain, {})
        return Web3.to_checksum_address(chain_known.get(token.upper(), token))
    
    async def get_quote(self, request: QuoteRequest) -> QuoteResponse:
        """Get best quote across fee tiers"""
        token_in = self._resolve_token(request.input_mint)
        token_out = self._resolve_token(request.output_mint)
        
        best_quote = None
        best_output = 0
        best_fee = 0
        
        for fee in self.FEE_TIERS:
            try:
                # Call quoter contract
                amount_out = self.quoter.functions.quoteExactInputSingle(
                    token_in, token_out, fee, request.amount, False
                ).call()
                
                if amount_out > best_output:
                    best_output = amount_out
                    best_fee = fee
                    best_quote = amount_out
            except Exception:
                continue
        
        if not best_quote:
            raise Exception("No quote available for any fee tier")
        
        # Calculate price impact (simplified)
        price_impact = 0.0  # Would need pool state for accurate calculation
        
        return QuoteResponse(
            input_amount=request.amount,
            output_amount=best_quote,
            price_impact_pct=price_impact,
            route=[{"fee": best_fee, "token_in": token_in, "token_out": token_out}],
            dex=DexProtocol.UNISWAP_V3,
            valid_for_seconds=20
        )
    
    async def execute_swap(self, quote: QuoteResponse) -> SwapResult:
        """Execute swap via Uniswap V3 Router"""
        route = quote.route[0]
        token_in = route["token_in"]
        token_out = route["token_out"]
        fee = route["fee"]
        
        # Check/approve token allowance
        if token_in != self.wrapped_native:
            await self._approve_token(token_in, self.config["router"], quote.input_amount)
        
        # Build swap transaction
        deadline = int(time.time()) + 300  # ponytail: epoch deadline, block-time abstraction if we later simulate
        min_out = int(quote.output_amount * 0.99)  # 1% additional slippage
        
        tx = self.router.functions.exactInputSingle({
            "tokenIn": token_in,
            "tokenOut": token_out,
            "fee": fee,
            "recipient": self.address,
            "deadline": deadline,
            "amountIn": quote.input_amount,
            "amountOutMinimum": min_out,
            "sqrtPriceLimitX96": 0
        }).build_transaction({
            "from": self.address,
            "gas": 300_000,
            "gasPrice": int(self.w3.eth.gas_price * self.gas_multiplier),
            "nonce": self.w3.eth.get_transaction_count(self.address),
            "chainId": self.config["chain_id"]
        })
        
        # Add value if buying with native
        if token_in == self.wrapped_native:
            tx["value"] = quote.input_amount
        
        # Sign and send
        signed = self.account.sign_transaction(tx)
        tx_hash = self.w3.eth.send_raw_transaction(signed.rawTransaction)
        tx_hash_hex = tx_hash.hex()
        
        # Wait for receipt
        receipt = await asyncio.get_event_loop().run_in_executor(
            None, lambda: self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
        )
        
        success = receipt.status == 1
        gas_used = receipt.gasUsed
        gas_price = receipt.effectiveGasPrice
        
        return SwapResult(
            success=success,
            signature=tx_hash_hex,
            input_amount=quote.input_amount,
            output_amount=quote.output_amount,
            fee_paid=gas_used * gas_price,
            price_impact_pct=quote.price_impact_pct,
            error=None if success else "Transaction reverted"
        )
    
    async def _approve_token(self, token: str, spender: str, amount: int):
        """Approve token spending"""
        token_contract = self.w3.eth.contract(
            address=Web3.to_checksum_address(token),
            abi=ERC20_ABI
        )
        
        # Check current allowance
        allowance = token_contract.functions.allowance(self.address, spender).call()
        if allowance >= amount:
            return
        
        # Approve
        tx = token_contract.functions.approve(spender, amount).build_transaction({
            "from": self.address,
            "gas": 100_000,
            "gasPrice": int(self.w3.eth.gas_price * self.gas_multiplier),
            "nonce": self.w3.eth.get_transaction_count(self.address),
            "chainId": self.config["chain_id"]
        })
        
        signed = self.account.sign_transaction(tx)
        tx_hash = self.w3.eth.send_raw_transaction(signed.rawTransaction)
        await asyncio.get_event_loop().run_in_executor(
            None, lambda: self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
        )
    
    async def get_native_balance(self) -> int:
        """Get native token balance (wei)"""
        return self.w3.eth.get_balance(self.address)
    
    async def get_token_balance(self, token: str) -> int:
        """Get ERC20 balance"""
        token_contract = self.w3.eth.contract(
            address=self._resolve_token(token),
            abi=ERC20_ABI
        )
        return token_contract.functions.balanceOf(self.address).call()


# =============================================================================
# FACTORY & MANAGER
# =============================================================================

class DexExecutorFactory:
    """Factory for creating chain-specific executors"""
    
    _executors: Dict[Chain, DexExecutor] = {}
    
    @classmethod
    def create(cls, chain: Chain, **kwargs) -> DexExecutor:
        if chain == Chain.SOLANA:
            return JupiterExecutor(**kwargs)
        elif chain in [Chain.ETHEREUM, Chain.BASE, Chain.ARBITRUM, Chain.POLYGON, Chain.BSC]:
            return UniswapV3Executor(chain=chain, **kwargs)
        else:
            raise ValueError(f"Unsupported chain: {chain}")
    
    @classmethod
    async def get_executor(cls, chain: Chain, **kwargs) -> DexExecutor:
        """Get or create executor (cached)"""
        if chain not in cls._executors:
            cls._executors[chain] = cls.create(chain, **kwargs)
            await cls._executors[chain].__aenter__()
        return cls._executors[chain]
    
    @classmethod
    async def close_all(cls):
        for exec in cls._executors.values():
            await exec.__aexit__(None, None, None)
        cls._executors.clear()


class MultiChainExecutor:
    """
    High-level executor that routes to the correct chain/DEX.
    Handles token discovery, routing, and execution.
    """
    
    def __init__(self, config: Dict[Chain, Dict] = None):
        self.config = config or {}
        self.executors: Dict[Chain, DexExecutor] = {}
    
    async def initialize(self):
        """Initialize all configured executors"""
        for chain, cfg in self.config.items():
            self.executors[chain] = DexExecutorFactory.create(chain, **cfg)
            await self.executors[chain].__aenter__()
    
    async def close(self):
        for exec in self.executors.values():
            await exec.__aexit__(None, None, None)
    
    def get_executor(self, chain: Chain) -> Optional[DexExecutor]:
        return self.executors.get(chain)
    
    async def quote(self, chain: Chain, request: QuoteRequest) -> QuoteResponse:
        executor = self.get_executor(chain)
        if not executor:
            raise ValueError(f"No executor for chain: {chain}")
        return await executor.get_quote(request)
    
    async def swap(self, chain: Chain, quote: QuoteResponse) -> SwapResult:
        executor = self.get_executor(chain)
        if not executor:
            raise ValueError(f"No executor for chain: {chain}")
        return await executor.execute_swap(quote)
    
    async def execute_snipe(
        self,
        token_address: str,
        chain: Chain,
        quote_token: str = "USDC",
        amount_usd: float = 100,
        slippage_bps: int = 200  # 2% for sniping
    ) -> SwapResult:
        """High-level snipe execution"""
        executor = self.get_executor(chain)
        if not executor:
            raise ValueError(f"No executor for chain: {chain}")
        
        # Resolve quote token
        if chain == Chain.SOLANA:
            quote_mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"  # USDC
            # For new tokens, we need to find the pool
            # Jupiter handles this automatically via routing
        else:
            quote_mint = self._resolve_token(chain, quote_token)
        
        # Get quote (amount in quote token smallest units)
        decimals = 6 if chain == Chain.SOLANA else 18  # USDC=6, WETH=18
        amount_raw = int(amount_usd * (10 ** decimals))
        
        request = QuoteRequest(
            input_mint=quote_mint,
            output_mint=token_address,
            amount=amount_raw,
            slippage_bps=slippage_bps
        )
        
        quote = await executor.get_quote(request)
        return await executor.execute_swap(quote)
    
    def _resolve_token(self, chain: Chain, symbol: str) -> str:
        """Resolve token symbol to address for chain"""
        if chain == Chain.SOLANA:
            # Would query Jupiter token list
            known = {
                "USDC": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                "SOL": "So11111111111111111111111111111111111111112",
                "USDT": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
            }
            return known.get(symbol.upper(), symbol)
        else:
            # Use Uniswap executor's resolver
            exec = self.executors.get(chain)
            if isinstance(exec, UniswapV3Executor):
                return exec._resolve_token(symbol)
            return symbol


# =============================================================================
# INTEGRATION WITH SNIPER BOT
# =============================================================================

class SniperExecutionEngine:
    """
    Bridges sniper bot detection with multi-chain execution.
    """
    
    def __init__(self, multi_executor: MultiChainExecutor, max_position_usd: float = 500):
        self.executor = multi_executor
        self.max_position_usd = max_position_usd
        self.active_positions: Dict[str, Dict] = {}
    
    async def execute_on_signal(self, token_info: "TokenInfo") -> SwapResult:
        """Execute snipe based on TokenInfo from sniper bot"""
        
        # Determine quote token (prefer USDC/USDT)
        if token_info.chain == "solana":
            quote_token = "USDC"
        else:
            quote_token = "USDC"
        
        # Execute
        result = await self.executor.execute_snipe(
            token_address=token_info.address,
            chain=Chain(token_info.chain),
            quote_token=quote_token,
            amount_usd=min(self.max_position_usd, token_info.liquidity_usd * 0.01),  # Max 1% of liquidity
            slippage_bps=300  # 3% for new tokens
        )
        
        if result.success:
            self.active_positions[token_info.address] = {
                "token": token_info,
                "entry_price": token_info.price_usd,
                "amount_in": result.input_amount,
                "amount_out": result.output_amount,
                "tx_hash": result.signature,
                "chain": token_info.chain
            }
        
        return result
    
    async def check_exit_conditions(self) -> List[SwapResult]:
        """Check and execute exits for active positions (TP/SL)"""
        results = []
        
        for token_addr, position in list(self.active_positions.items()):
            token = position["token"]
            chain = Chain(token.chain)
            exec = self.executor.get_executor(chain)
            
            if not exec:
                continue
            
            # Get current price (would need price feed)
            # For now, placeholder logic
            current_price = token.price_usd  # Would fetch real-time
            entry_price = position["entry_price"]
            pnl_pct = (current_price - entry_price) / entry_price
            
            # Exit conditions
            should_exit = False
            exit_reason = ""
            
            if pnl_pct >= 0.5:  # 50% take profit
                should_exit = True
                exit_reason = "take_profit_50pct"
            elif pnl_pct <= -0.3:  # 30% stop loss
                should_exit = True
                exit_reason = "stop_loss_30pct"
            
            if should_exit:
                # Sell back to quote token
                quote_token = "USDC"
                if chain == Chain.SOLANA:
                    quote_mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
                else:
                    quote_mint = self.executor._resolve_token(chain, quote_token)
                
                request = QuoteRequest(
                    input_mint=token_addr,
                    output_mint=quote_mint,
                    amount=position["amount_out"],
                    slippage_bps=500  # 5% for exit
                )
                
                quote = await exec.get_quote(request)
                result = await exec.execute_swap(quote)
                
                if result.success:
                    del self.active_positions[token_addr]
                    results.append(result)
        
        return results


# =============================================================================
# USAGE EXAMPLE
# =============================================================================

async def example_usage():
    """Example: Initialize and use multi-chain executor"""
    
    # Configuration from environment
    config = {
        Chain.SOLANA: {
            "rpc_url": os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com"),
            "private_key": os.getenv("SOLANA_PRIVATE_KEY"),
            "priority_fee_lamports": 200_000
        },
        Chain.BASE: {
            "rpc_url": os.getenv("BASE_RPC_URL", "https://mainnet.base.org"),
            "private_key": os.getenv("BASE_PRIVATE_KEY") or os.getenv("EVM_PRIVATE_KEY"),
            "gas_multiplier": 1.5
        }
    }
    
    # Initialize
    multi_exec = MultiChainExecutor(config)
    await multi_exec.initialize()
    
    try:
        # Example: Quote SOL -> USDC on Solana
        quote = await multi_exec.quote(Chain.SOLANA, QuoteRequest(
            input_mint="So11111111111111111111111111111111111111112",  # SOL
            output_mint="EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
            amount=1_000_000_000,  # 1 SOL in lamports
            slippage_bps=100
        ))
        print(f"Quote: 1 SOL = {quote.output_amount / 1e6:.2f} USDC")
        print(f"Price impact: {quote.price_impact_pct:.2f}%")
        
        # Example: Snipe new token on Base
        # result = await multi_exec.execute_snipe(
        #     token_address="0x...",  # New token address
        #     chain=Chain.BASE,
        #     amount_usd=100
        # )
        
    finally:
        await multi_exec.close()


if __name__ == "__main__":
    asyncio.run(example_usage())
