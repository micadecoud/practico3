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
CMD ["python3", "src/main_cartelera.py"]
