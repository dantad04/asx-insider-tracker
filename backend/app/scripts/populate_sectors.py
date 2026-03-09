"""Populate sector data for ASX-listed companies.

Maps well-known ASX tickers to their GICS sectors. Any company
not in the mapping that currently has no sector is set to 'Other'.

Usage:
    docker-compose exec backend python -m app.scripts.populate_sectors
"""

import asyncio
import logging

from sqlalchemy import select

from app.database import async_session
from app.models.company import Company

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# GICS sector mapping for major ASX tickers
SECTOR_MAP: dict[str, str] = {
    # ── Financials ─────────────────────────────────────────────────────────
    "CBA": "Financials", "WBC": "Financials", "ANZ": "Financials", "NAB": "Financials",
    "MQG": "Financials", "SUN": "Financials", "QBE": "Financials", "IAG": "Financials",
    "BEN": "Financials", "BOQ": "Financials", "AMP": "Financials", "PPT": "Financials",
    "ASX": "Financials", "IFL": "Financials", "MFG": "Financials", "CGF": "Financials",
    "GQG": "Financials", "HUB": "Financials", "NWL": "Financials", "AFG": "Financials",
    "PDL": "Financials", "AHY": "Financials", "NSX": "Financials", "VUK": "Financials",
    "CRZ": "Financials", "FNP": "Financials", "EQT": "Financials", "MPL": "Financials",
    "NIB": "Financials",
    # ── Materials ──────────────────────────────────────────────────────────
    "BHP": "Materials", "RIO": "Materials", "FMG": "Materials", "NCM": "Materials",
    "S32": "Materials", "NST": "Materials", "EVN": "Materials", "OZL": "Materials",
    "IGO": "Materials", "LYC": "Materials", "MIN": "Materials", "AWC": "Materials",
    "SFR": "Materials", "SGM": "Materials", "BSL": "Materials",
    "PLS": "Materials", "AKE": "Materials", "CXO": "Materials", "GL1": "Materials",
    "NIC": "Materials", "CHN": "Materials", "FFX": "Materials",
    "ILU": "Materials", "SYR": "Materials", "GXY": "Materials", "ORE": "Materials",
    "SRL": "Materials", "AGY": "Materials", "CLQ": "Materials", "CNB": "Materials",
    # ── Healthcare ─────────────────────────────────────────────────────────
    "CSL": "Healthcare", "RMD": "Healthcare", "COH": "Healthcare", "SHL": "Healthcare",
    "RHC": "Healthcare", "HLS": "Healthcare", "PME": "Healthcare", "NAN": "Healthcare",
    "MSB": "Healthcare", "NEU": "Healthcare", "CUV": "Healthcare", "PNV": "Healthcare",
    "ARX": "Healthcare", "RAC": "Healthcare", "AOV": "Healthcare", "ACL": "Healthcare",
    "CAJ": "Healthcare", "TLX": "Healthcare", "SOM": "Healthcare", "EBR": "Healthcare",
    "IPD": "Healthcare", "AVH": "Healthcare", "IRI": "Healthcare",
    # ── Energy ─────────────────────────────────────────────────────────────
    "WDS": "Energy", "STO": "Energy", "ORG": "Energy", "BPT": "Energy",
    "KAR": "Energy", "COE": "Energy", "NHC": "Energy", "WHC": "Energy",
    "YAL": "Energy", "SEN": "Energy", "VEA": "Energy",
    "CVN": "Energy", "HZN": "Energy", "FAR": "Energy", "VPT": "Energy",
    # ── Industrials ────────────────────────────────────────────────────────
    "QAN": "Industrials", "TCL": "Industrials", "BXB": "Industrials",
    "WOR": "Industrials", "DOW": "Industrials", "QUB": "Industrials",
    "CIM": "Industrials", "ALX": "Industrials", "MMA": "Industrials",
    "AZJ": "Industrials", "SVW": "Industrials", "WGX": "Industrials",
    "NHH": "Industrials", "CWY": "Industrials", "SWM": "Industrials",
    # ── Consumer Staples ───────────────────────────────────────────────────
    "WOW": "Consumer Staples", "WES": "Consumer Staples", "COL": "Consumer Staples",
    "EDV": "Consumer Staples", "TWE": "Consumer Staples", "A2M": "Consumer Staples",
    "BGA": "Consumer Staples", "CGC": "Consumer Staples", "ING": "Consumer Staples",
    "FLN": "Consumer Staples", "TSI": "Consumer Staples",
    # ── Consumer Discretionary ─────────────────────────────────────────────
    "JBH": "Consumer Discretionary", "HVN": "Consumer Discretionary",
    "MYR": "Consumer Discretionary", "SUL": "Consumer Discretionary",
    "ARB": "Consumer Discretionary", "ALL": "Consumer Discretionary",
    "TAH": "Consumer Discretionary", "PMV": "Consumer Discretionary",
    "OML": "Consumer Discretionary", "SKC": "Consumer Discretionary",
    "FLT": "Consumer Discretionary", "WEB": "Consumer Discretionary",
    "CTD": "Consumer Discretionary", "VGL": "Consumer Discretionary",
    # ── Technology ─────────────────────────────────────────────────────────
    "XRO": "Technology", "WTC": "Technology", "CPU": "Technology",
    "APX": "Technology", "MP1": "Technology", "ALU": "Technology",
    "NXT": "Technology", "TNE": "Technology", "TYR": "Technology",
    "AD8": "Technology", "LNK": "Technology", "HSN": "Technology",
    "BTH": "Technology", "DDR": "Technology", "DUB": "Technology",
    "DSK": "Technology", "PGL": "Technology", "IVZ": "Technology",
    # ── Real Estate ────────────────────────────────────────────────────────
    "GPT": "Real Estate", "SCG": "Real Estate", "CHC": "Real Estate",
    "MGR": "Real Estate", "GMG": "Real Estate", "VCX": "Real Estate",
    "ABP": "Real Estate", "CLW": "Real Estate", "HMC": "Real Estate",
    "NSR": "Real Estate", "DXS": "Real Estate", "CIP": "Real Estate",
    "BWP": "Real Estate", "WPR": "Real Estate", "ARF": "Real Estate",
    "GDI": "Real Estate", "URW": "Real Estate",
    # ── Utilities ──────────────────────────────────────────────────────────
    "APA": "Utilities", "AGL": "Utilities", "AST": "Utilities",
    "ENV": "Utilities", "MEZ": "Utilities", "GNE": "Utilities",
    # ── Communication Services ─────────────────────────────────────────────
    "TLS": "Communication Services", "TPG": "Communication Services",
    "NWS": "Communication Services", "REA": "Communication Services",
    "CAR": "Communication Services", "SEK": "Communication Services",
    "IEL": "Communication Services", "SLC": "Communication Services",
    "NEC": "Communication Services", "SXL": "Communication Services",
    "OML": "Communication Services",
}


async def main() -> None:
    async with async_session() as session:
        result = await session.execute(select(Company))
        companies = result.scalars().all()

        mapped = 0
        othered = 0

        for company in companies:
            sector = SECTOR_MAP.get(company.ticker)
            if sector:
                company.sector = sector
                mapped += 1
            elif company.sector is None:
                company.sector = "Other"
                othered += 1

        await session.commit()

    print(f"Done: {mapped} tickers mapped to sectors, {othered} set to 'Other'")
    logger.info(f"populate_sectors complete: {mapped} mapped, {othered} set to Other")


if __name__ == "__main__":
    asyncio.run(main())
