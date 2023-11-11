# # Usar una imagen base de Python
# FROM python:3.9

# # Establecer el directorio de trabajo en el contenedor
# WORKDIR /app

# # Copiar los archivos requeridos al contenedor
# COPY generador.py /app
# COPY requirements.txt /app

# # Instalar las dependencias del proyecto
# RUN pip install --no-cache-dir -r requirements.txt

# # Comando para ejecutar el script
# CMD ["python", "./generador.py"]
FROM python:3.12-bookworm
WORKDIR /app
COPY generador.py /app
COPY requirements.txt /app
RUN apt-get update && apt-get install nano -y
RUN pip install networkx