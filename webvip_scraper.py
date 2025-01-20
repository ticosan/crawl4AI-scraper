import os
import asyncio
import requests
from xml.etree import ElementTree
from typing import List
from urllib.parse import urlparse
from datetime import datetime, timezone
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode

# Configuración del directorio de salida
OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

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

async def save_content_to_file(url: str, content: str):
    """Guarda el contenido extraído de una URL en un archivo único."""
    # Generar un nombre de archivo basado en la URL
    filename = os.path.join(
        OUTPUT_DIR, 
        urlparse(url).path.strip("/").replace("/", "_") + ".txt"
    )
    
    # Guardar el contenido en un archivo
    with open(filename, "w", encoding="utf-8") as file:
        file.write(f"URL: {url}\n")
        file.write(f"Fecha: {datetime.now(timezone.utc).isoformat()}\n\n")
        file.write(content)
    print(f"Contenido guardado en: {filename}")

async def crawl_and_save(urls: List[str], max_concurrent: int = 5):
    """Rastrea múltiples URLs en paralelo y guarda su contenido."""
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
                    await save_content_to_file(url, result.markdown_v2.raw_markdown)
                else:
                    print(f"Failed to crawl {url}: {result.error_message}")

        await asyncio.gather(*[process_url(url) for url in urls])
    finally:
        await crawler.close()

async def main(sitemap_url: str = None, single_url: str = None, limit_urls: int = None):
    """Función principal para extraer contenido de un sitemap o una URL única."""
    urls = []

    if single_url:
        # Si se pasa una URL individual, procesarla directamente
        urls = [single_url]
        print(f"Procesando URL individual: {single_url}")
    elif sitemap_url:
        # Si se pasa un sitemap, obtener las URLs del sitemap
        urls = get_urls_from_sitemap(sitemap_url)
        if not urls:
            print("No se encontraron URLs en el sitemap.")
            return

        if limit_urls:
            urls = urls[:limit_urls]
            print(f"Procesando las primeras {limit_urls} URLs del sitemap.")
        else:
            print(f"Procesando todas las {len(urls)} URLs del sitemap.")
    else:
        print("Error: Debes proporcionar un sitemap o una URL única.")
        return

    await crawl_and_save(urls)

if __name__ == "__main__":
    import argparse

    # Agregar argumentos para elegir el origen de las URLs
    parser = argparse.ArgumentParser(description="Crawl URLs from a sitemap or a single URL and save their content.")
    parser.add_argument(
        "--sitemap",
        type=str,
        default=None,
        help="URL del sitemap XML desde donde extraer las URLs."
    )
    parser.add_argument(
        "--url",
        type=str,
        default=None,
        help="URL individual a procesar."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Número máximo de URLs a procesar desde el sitemap. Si no se especifica, procesará todas las URLs."
    )
    args = parser.parse_args()

    # Ejecutar el script con los argumentos proporcionados
    asyncio.run(main(sitemap_url=args.sitemap, single_url=args.url, limit_urls=args.limit))
