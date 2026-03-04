"""
Deduplicate directors table by merging case variations and merging trades.

Usage:
    docker-compose exec backend python -m app.scripts.deduplicate_directors
"""

import asyncio
import logging
from sqlalchemy import select, func, and_

from app.database import async_session
from app.models.director import Director
from app.models.trade import Trade
from app.models.director_company import DirectorCompany

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


async def main():
    """Deduplicate directors by merging case variations."""

    async with async_session() as session:
        # Find directors with duplicate names (case-insensitive)
        from sqlalchemy import text

        result = await session.execute(
            text("""
            SELECT
                lower(full_name) as name_lower,
                count(id) as count,
                array_agg(id) as ids
            FROM directors
            GROUP BY lower(full_name)
            HAVING count(id) > 1
            """)
        )

        duplicates = result.all()
        logger.info(f"Found {len(duplicates)} duplicate director groups")

        merged_count = 0
        for row in duplicates:
            name_lower, count, ids = row
            if not ids or len(ids) < 2:
                continue

            # Keep the first ID, merge all others into it
            primary_id = ids[0]
            merge_ids = ids[1:]

            logger.info(f"Merging {len(merge_ids)} duplicates into {primary_id}: {name_lower}")

            # Move all trades from duplicate directors to primary
            for dup_id in merge_ids:
                await session.execute(
                    Trade.__table__.update()
                    .where(Trade.director_id == dup_id)
                    .values(director_id=primary_id)
                )

                # Move director_companies relationships
                await session.execute(
                    DirectorCompany.__table__.update()
                    .where(DirectorCompany.director_id == dup_id)
                    .values(director_id=primary_id)
                )

                # Delete the duplicate director
                await session.execute(
                    Director.__table__.delete()
                    .where(Director.id == dup_id)
                )

                merged_count += 1

        await session.commit()
        logger.info(f"✓ Merged {merged_count} duplicate directors")


if __name__ == "__main__":
    asyncio.run(main())
