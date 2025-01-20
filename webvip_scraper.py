import os
import asyncio
import requests
from xml.etree import ElementTree
from typing import List, Dict, Any
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import urlparse
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode

# Configuración del directorio de salida
OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

@dataclass
class ProcessedChunk:
    url: str
    chunk_number: int
    title: str
    summary: str
    content: str
    metadata: Dict[str, Any]

def chunk_text(text: str, chunk_size: int = 5000) -> List[str]:
    """Divide texto en fragmentos manejables."""
    chunks = []
    start = 0
    text_length = len(text)

    while start < text_length:
        end = start + chunk_size
        if end >= text_length:
            chunks.append(text[start:].strip())
            break

        chunk = text[start:end]
        last_break = max(chunk.rfind('\n\n'), chunk.rfind('. '), chunk.rfind('```'))
        if last_break > chunk_size * 0.3:
            end = start + last_break + 1

        chunks.append(text[start:end].strip())
        start = end

    return chunks

async def get_title_and_summary(chunk: str, url: str) -> Dict[str, str]:
    """Simula la extracción de título y resumen."""
    return {
        "title": f"Title for {url} - Chunk {chunk[:30]}",
        "summary": f"Summary of content: {chunk[:100]}..."
    }

async def process_chunk(chunk: str, chunk_number: int, url: str) -> ProcessedChunk:
    """Procesa un fragmento de texto."""
    extracted = await get_title_and_summary(chunk, url)
    metadata = {
        "chunk_size": len(chunk),
        "crawled_at": datetime.now(timezone.utc).isoformat(),
        "url_path": urlparse(url).path
    }
    return ProcessedChunk(
        url=url,
        chunk_number=chunk_number,
        title=extracted["title"],
        summary=extracted["summary"],
        content=chunk,
        metadata=metadata
    )

async def save_chunks_to_files(url: str, chunks: List[ProcessedChunk]):
    """Guarda todos los fragmentos en archivos separados."""
    base_filename = os.path.join(OUTPUT_DIR, urlparse(url).path.strip("/").replace("/", "_"))
    for chunk in chunks:
        filename = f"{base_filename}_chunk_{chunk.chunk_number}.txt"
        with open(filename, "w", encoding="utf-8") as file:
            file.write(f"URL: {chunk.url}\n")
            file.write(f"Chunk {chunk.chunk_number}\n")
            file.write(f"Title: {chunk.title}\n")
            file.write(f"Summary: {chunk.summary}\n")
            file.write(f"Content:\n{chunk.content}\n")
        print(f"Saved chunk {chunk.chunk_number} to {filename}")

async def process_and_save_document(url: str, markdown: str):
    """Procesa un documento y guarda todos los fragmentos en archivos."""
    chunks = chunk_text(markdown)
    processed_chunks = await asyncio.gather(
        *[process_chunk(chunk, i, url) for i, chunk in enumerate(chunks)]
    )
    await save_chunks_to_files(url, processed_chunks)

async def crawl_parallel(urls: List[str], max_concurrent: int = 5):
    """Rastrea múltiples URLs en paralelo con límite de concurrencia."""
    browser_config = BrowserConfig(
        headless=True,
        verbose=False,
        extra_args=["--disable-gpu", "--disable-dev-shm-usage", "--no-sandbox"],
    )
    crawl_config = CrawlerRunConfig(cache_mode=CacheMode.BYPASS)

    crawler = AsyncWebCrawler(config=browser_config)
    await crawler.start()

    try:
        semaphore = asyncio.Semaphore(max_concurrent)

        async def process_url(url: str):
            async with semaphore:
                result = await crawler.arun(url=url, config=crawl_config, session_id="session1")
                if result.success:
                    print(f"Successfully crawled: {url}")
                    await process_and_save_document(url, result.markdown_v2.raw_markdown)
                else:
                    print(f"Failed to crawl {url}: {result.error_message}")

        await asyncio.gather(*[process_url(url) for url in urls])
    finally:
        await crawler.close()

def get_urls_from_sitemap(sitemap_url: str) -> List[str]:
    """Obtiene y analiza el sitemap XML para extraer URLs."""
    try:
        response = requests.get(sitemap_url)
        response.raise_for_status()

        # Analizar el XML
        root = ElementTree.fromstring(response.content)

        # Detectar espacios de nombres dinámicamente
        namespace = {'ns': root.tag.split('}')[0].strip('{')} if '}' in root.tag else {}

        # Buscar todas las etiquetas <loc>
        return [loc.text for loc in root.findall('.//ns:loc', namespace)]
    except Exception as e:
        print(f"Error fetching sitemap: {e}")
        return []

async def main():
    sitemap_url = "https://www.hellofresh.es/sitemap_recipe_pages.xml"
    urls = get_urls_from_sitemap(sitemap_url)
    if not urls:
        print("No URLs found in sitemap.")
        return

    print(f"Found {len(urls)} URLs to crawl.")
    await crawl_parallel(urls)

if __name__ == "__main__":
    asyncio.run(main())
