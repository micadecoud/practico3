[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/o8BZ7fxD)
# Práctico 2 - Web Scraping con Python y Docker

En este práctico se implementará un web scraper utilizando Python y Docker para extraer información de propiedades inmobiliarias de un sitio web.

## Objetivos
- Implementar un web scraper utilizando Playwright
- Empaquetar la aplicación en un contenedor Docker

## Estructura del Proyecto

El proyecto está organizado de la siguiente manera:

- `src/main.py`: Punto de entrada de la aplicación
- `src/scrapers/gallito.py`: Implementación del scraper
- `src/structs/property.py`: Modelos de datos con Pydantic
- `Dockerfile`: Configuración del contenedor Docker
- `requirements.in`: Dependencias del proyecto

## Índice de Contenidos

1. [Playwright](#1-playwright)
  
   1.1 [¿Qué es Playwright?](#11-qué-es-playwright)

   1.2 [¿Para qué sirve Playwright?](#12-para-qué-sirve-playwright)

   1.3 [¿Qué ventajas y desventajas tiene Playwright sobre otros frameworks de scraping?](#13-qué-ventajas-y-desventajas-tiene-playwright-sobre-otros-frameworks-de-scraping)

   1.4 [¿Cómo funciona Playwright?](#14-cómo-funciona-playwright)

   1.5 [¿Cómo instalar Playwright?](#15-cómo-instalar-playwright)

   1.6 [¿Cómo usar Playwright?](#16-cómo-usar-playwright)

2. [Scraping local](#2-scraping-local)

   2.1 [Instalación de dependencias](#21-instalación-de-dependencias)

   2.2 [Ejecución del scraper](#22-ejecución-del-scraper)

   2.3 [Almacenamiento de datos](#23-almacenamiento-de-datos)

3. [Dockerización](#3-dockerización)

   3.1 [Creación de Dockerfile](#31-creación-de-dockerfile)

   3.2 [Ejecución de Docker](#32-ejecución-de-docker)


## 1. Playwright

### 1.1. ¿Qué es Playwright?

Playwright es un framework de automatización de navegadores desarrollado por Microsoft. Permite controlar programáticamente los navegadores Chrome, Firefox y Safari, facilitando la automatización de pruebas y web scraping.

### 1.2 ¿Para qué sirve Playwright?

Playwright se utiliza principalmente para:
- Automatización de pruebas end-to-end
- Web scraping y extracción de datos
- Generación de screenshots y PDFs
- Automatización de tareas en navegadores
- Testing de aplicaciones web

### 1.3 ¿Qué ventajas y desventajas tiene Playwright sobre otros frameworks de scraping?

Ventajas:
- Soporte para múltiples navegadores
- API moderna y fácil de usar
- Pensado para nuevas versiones de navegadores
- Manejo automático de esperas
- Soporte para páginas dinámicas (JavaScript)

Desventajas:
- Mayor consumo de recursos
- No sirve para automatizar aplicaciones móviles

### 1.4 ¿Cómo funciona Playwright?

Playwright funciona:
1. Iniciando una instancia del navegador
2. Creando un contexto aislado
3. Abriendo páginas dentro del contexto
4. Interactuando con elementos mediante selectores
5. Extrayendo información del DOM

### 1.5 ¿Cómo instalar Playwright?

```bash
# Instalar el paquete de Python
pip install playwright

# Instalar los navegadores
playwright install

# Instalar dependencias
playwright install-deps
```

### 1.6 ¿Cómo usar Playwright?

Ejemplo básico de uso:

```python
from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    browser = p.chromium.launch()
    page = browser.new_page()
    page.goto('https://ejemplo.com')
    title = page.title()
    browser.close()
```

## 2. Scraping local

### 2.1 Instalación de dependencias

1. Crear y activar entorno virtual:
```bash
virtualenv venv -p $(which python3.11)
source venv/bin/activate
```

2. Instalar dependencias:
```bash
pip install -r requirements.txt
```

### 2.2 Ejecución del scraper

1. Configurar parámetros en `src/settings/config.yml`
2. Ejecutar el scraper:
```bash
python src/main.py
```

### 2.3 Almacenamiento de datos

Los datos se almacenan en:
- `data/scraped_data/properties/`: Archivos JSONL con metadatos
- `data/scraped_data/images/`: Imágenes descargadas

## 3. Dockerización

### 3.1 Creación de Dockerfile

El Dockerfile incluye:
- Imagen base de Python
- Instalación de dependencias
- Configuración de Playwright
- Volúmenes para datos persistentes

```dockerfile
# Imagen base
FROM python:3.11-slim

# Crea y fija directorio de trabajo
WORKDIR /app

# Crear archivo de dependencias
COPY requirements.in requirements.in
RUN pip install -U pip
RUN pip install pip-tools
RUN pip-compile requirements.in

# Instalar dependencias de nuestro proyecto
RUN pip install -r requirements.txt && \
    playwright install && \
    playwright install-deps

# Copiar archivos necesarios
COPY src src

# Ejecuta bash para usar la línea de comandos
CMD ["python3", "src/main.py"]
```

### 3.2 Ejecución de Docker

1. Construir la imagen:
```bash
docker build -t property-scraper .
```

2. Ejecutar el contenedor:
```bash
docker run --name scraper_container -v "$(pwd)/data/scraped_data:/app/data/scraped_data" property-scraper
```
