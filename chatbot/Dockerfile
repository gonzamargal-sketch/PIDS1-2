# Servicios chatbot y chat-ui (Parte 3). El código no se copia:
# docker-compose.yml monta ./chatbot en /app/chatbot.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app
COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

EXPOSE 8001 8501
CMD ["uvicorn", "chatbot.servidor:app", "--host", "0.0.0.0", "--port", "8001"]
