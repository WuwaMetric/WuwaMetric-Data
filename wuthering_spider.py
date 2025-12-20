import asyncio
import argparse
import json
import logging
import sys
from pathlib import Path
from typing import List, Any, Optional, Set

import httpx
from tqdm import tqdm

BASE_URL = "https://api-v2.encore.moe"
API_VER_PARAM_KEY = "ver"
API_VER_PARAM_VALUE = "latest"
HTTP_TIMEOUT = 30.0
MAX_RETRIES = 3
RETRY_DELAY = 1.0
MAX_CONCURRENCY = 20
MAX_KEEPALIVE = 20
HTTP_NOT_FOUND = 404

META_ENDPOINT = "new"

DEFAULT_LANGS = [
    "en",
    "zh-Hans",
    "zh-Hant",
    "ja",
    "ko",
    "de",
    "es",
    "fr",
    "id",
    "pt",
    "ru",
    "th",
    "vi",
]

FOLDER_LANG_MAP = {
    "en": "en-us",
    "zh-Hans": "zh-cn",
    "zh-Hant": "zh-tw",
    "ja": "ja-jp",
    "ko": "ko-kr",
    "de": "de-de",
    "es": "es-es",
    "fr": "fr-fr",
    "id": "id-id",
    "pt": "pt-pt",
    "ru": "ru-ru",
    "th": "th-th",
    "vi": "vi-vn",
}

CATEGORY_MAP = {
    "character": "roleList",
    "weapon": "weapons",
    "echo": "Echo",
    "monster": "monsterList",
    "item": "itemList",
    "namecard": "namecardList",
    "title": "titleList",
    "toa": "seasons",
    "story": "storyTypes",
    "info": "infoList",
    "term": "termList",
    "phone": None,
}

logger = logging.getLogger("EncoreSpider")


class EncoreSpider:
    def __init__(
        self,
        output_dir: str,
        langs: List[str],
        concurrency: int = MAX_CONCURRENCY,
        force: bool = False,
        soft_force: bool = False,
        test: bool = False,
    ):
        self.output_dir = Path(output_dir)
        self.langs = langs
        self.concurrency = concurrency
        self.force = force
        self.soft_force = soft_force
        self.test = test
        self.client = httpx.AsyncClient(
            base_url=BASE_URL,
            timeout=HTTP_TIMEOUT,
            limits=httpx.Limits(
                max_keepalive_connections=MAX_KEEPALIVE, max_connections=concurrency
            ),
            follow_redirects=True,
        )
        self.semaphore = asyncio.Semaphore(concurrency)

    async def close(self):
        await self.client.aclose()

    async def _fetch(self, url: str) -> Optional[Any]:
        for _ in range(MAX_RETRIES):
            try:
                async with self.semaphore:
                    response = await self.client.get(
                        url, params={API_VER_PARAM_KEY: API_VER_PARAM_VALUE}
                    )
                    if response.status_code == HTTP_NOT_FOUND:
                        return None
                    response.raise_for_status()
                    return response.json()
            except httpx.HTTPError as e:
                logger.debug(f"HTTP Error fetching {url}: {e}")
                await asyncio.sleep(RETRY_DELAY)
            except Exception as e:
                logger.debug(f"Error fetching {url}: {e}")
                return None
        return None

    def _should_save(self, data: Any, path: Path) -> bool:
        if self.force:
            return True

        if not path.exists():
            return True

        if self.soft_force:
            try:
                with open(path, "r", encoding="utf-8") as f:
                    existing_data = json.load(f)
                return json.dumps(data, sort_keys=True) != json.dumps(
                    existing_data, sort_keys=True
                )
            except Exception:
                return True

        return False

    def _save_json(self, data: Any, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def _extract_ids(self, category: str, data: Any) -> List[str]:
        ids: Set[str] = set()
        try:
            items = []
            if category == "phone":
                if isinstance(data, list):
                    items = data
            elif category == "story":
                story_types = data.get("storyTypes", [])
                for st in story_types:
                    for group in st.get("Stories", []):
                        items.extend(group.get("Stories", []))
            else:
                key = CATEGORY_MAP.get(category)
                if key and isinstance(data, dict):
                    items = data.get(key, [])

            if not isinstance(items, list):
                return []

            for item in items:
                item_id = item.get("Id") or item.get("id") or item.get("TypeId")
                if item_id is not None:
                    ids.add(str(item_id))
        except Exception:
            pass
        return list(ids)

    async def _process_metadata(self, lang: str):
        folder_name = FOLDER_LANG_MAP.get(lang, lang)
        
        url = f"/{lang}/{META_ENDPOINT}"
        data = await self._fetch(url)

        if data:
            file_path = self.output_dir / folder_name / f"{META_ENDPOINT}.json"
            if self._should_save(data, file_path):
                self._save_json(data, file_path)

    async def _process_detail(
        self, lang: str, category: str, item_id: str, file_bar: tqdm
    ):
        folder_name = FOLDER_LANG_MAP.get(lang, lang)
        
        file_path = self.output_dir / folder_name / category / f"{item_id}.json"

        try:
            if file_path.exists() and not self.force and not self.soft_force:
                return

            url = f"/{lang}/{category}/{item_id}"
            data = await self._fetch(url)

            if data:
                if self._should_save(data, file_path):
                    self._save_json(data, file_path)
        finally:
            file_bar.update(1)

    async def _process_category(
        self, lang: str, category: str, cat_bar: tqdm, file_bar: tqdm
    ):
        try:
            folder_name = FOLDER_LANG_MAP.get(lang, lang)
            
            index_url = f"/{lang}/{category}"
            index_data = await self._fetch(index_url)

            if not index_data:
                return

            index_path = self.output_dir / folder_name / category / "index.json"

            if self._should_save(index_data, index_path):
                self._save_json(index_data, index_path)

            item_ids = self._extract_ids(category, index_data)

            if self.test and item_ids:
                item_ids = item_ids[:1]

            if item_ids:
                file_bar.total += len(item_ids)
                file_bar.refresh()

                tasks = [
                    self._process_detail(lang, category, i, file_bar) for i in item_ids
                ]
                await asyncio.gather(*tasks)

        finally:
            cat_bar.update(1)

    async def _process_metadata_task(self, lang: str, cat_bar: tqdm):
        try:
            await self._process_metadata(lang)
        finally:
            cat_bar.update(1)

    async def run(self):
        total_cats = len(self.langs) * (len(CATEGORY_MAP) + 1)

        with tqdm(
            total=total_cats, desc="Categories", position=0, leave=True
        ) as cat_bar:
            with tqdm(total=0, desc="Files", position=1, leave=True) as file_bar:
                tasks = []
                for lang in self.langs:
                    tasks.append(self._process_metadata_task(lang, cat_bar))

                    for category in CATEGORY_MAP.keys():
                        tasks.append(
                            self._process_category(lang, category, cat_bar, file_bar)
                        )

                await asyncio.gather(*tasks)


async def main():
    parser = argparse.ArgumentParser(description="Encore.moe Data Spider (v2)")
    parser.add_argument("--out", type=str, default="data", help="Output directory")
    parser.add_argument(
        "--langs",
        type=str,
        default=",".join(DEFAULT_LANGS),
        help="Comma-separated languages",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Hard force: Always redownload and overwrite",
    )
    parser.add_argument(
        "--soft-force",
        action="store_true",
        help="Soft force: Download and overwrite only if content differs",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Test mode: Download index and 1 detail item per category",
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    log_level = logging.INFO if args.debug else logging.WARNING
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    selected_langs = [l.strip() for l in args.langs.split(",") if l.strip()]

    spider = EncoreSpider(
        output_dir=args.out,
        langs=selected_langs,
        force=args.force,
        soft_force=args.soft_force,
        test=args.test,
    )

    try:
        if args.debug:
            logger.info(f"Starting spider for {len(selected_langs)} languages...")
        else:
            print(f"Starting spider for {len(selected_langs)} languages...")

        await spider.run()
        print("\nDone.")
    except KeyboardInterrupt:
        print("\nInterrupted.")
    finally:
        await spider.close()


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())