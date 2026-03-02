# 1. Image de base légère avec Python 3.11
FROM python:3.11-slim

# 2. On évite les fichiers .pyc et on force l'affichage des logs en temps réel
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# 3. Dossier de travail dans le container
WORKDIR /app

# 4. On installe les dépendances système (si nécessaire) et Python
# On copie le fichier depuis la racine de ton PC
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. On copie tout le contenu de ton dossier 'src' vers le dossier '/app' du container
COPY ./src .

# 6. La commande magique qui lance ton flux de données
CMD ["python", "producer.py"]
