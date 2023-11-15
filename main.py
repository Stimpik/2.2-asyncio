import asyncio
from more_itertools import chunked

import aiohttp

from models import engine, Base, Session, SwapiPeople

MAX_CHUNK_SIZE = 10


async def get_people(person_id):
    session = aiohttp.ClientSession()
    response = await session.get(f"https://swapi.dev/api/people/{person_id}")
    json_data = await response.json()
    await session.close()
    return json_data


async def get_links(links):
    session = aiohttp.ClientSession()
    if links:
        links_list = [await session.get(link) for link in links]
        content_list = [await r.json(content_type=None) for r in links_list]
        result = ", ".join(
            [element.get("title") or element.get("name") for element in content_list]
        )
        await session.close()
        return result
    else:
        await session.close()
        return None


async def insert_to_db(results):
    async with Session() as session:
        swapi_people = [
            SwapiPeople(
                name=item.get("name"),
                birth_year=item.get("birth_year"),
                eye_color=item.get("eye_color"),
                films=await get_links(item.get("films")),
                gender=item.get("gender"),
                hair_color=item.get("hair_color"),
                height=item.get("height"),
                homeworld=item.get("homeworld"),
                mass=item.get("mass"),
                skin_color=item.get("skin_color"),
                species=await get_links(item.get("species")),
                starships=await get_links(item.get("starships")),
                vehicles=await get_links(item.get("vehicles")),
            )
            for item in results
        ]

        session.add_all(swapi_people)
        await session.commit()


async def main():
    async with engine.begin() as con:
        await con.run_sync(Base.metadata.create_all)

    for ids in chunked(range(1, 90), MAX_CHUNK_SIZE):
        coros = [get_people(person_id) for person_id in ids]
        data = await asyncio.gather(*coros)
        asyncio.create_task(insert_to_db(data))
    current_task = asyncio.current_task()
    tasks_sets = asyncio.all_tasks()
    tasks_sets.remove(current_task)
    await asyncio.gather(*tasks_sets)
    await engine.dispose()


asyncio.run(main())
