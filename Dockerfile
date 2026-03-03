# Image de base légère avec Python 3.11
FROM python:3.11-slim

# On évite les fichiers .pyc et on force l'affichage des logs en temps réel
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Dossier de travail dans le container
WORKDIR /app

# On installe les dépendances système (si nécessaire) et Python
# On copie le fichier depuis la racine de ton PC
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# On copie tout le contenu de ton dossier 'src' vers le dossier '/app' du container
COPY ./src /app

# On s'assure que Python trouve bien les fichiers dans /app
ENV PYTHONPATH=/app

# La commande magique qui lance ton flux de données
CMD ["python", "kafkaProducer.py"]
