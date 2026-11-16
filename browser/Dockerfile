FROM python:3.12-slim
WORKDIR /app
COPY gridbot.html index.html
COPY serve.py serve.py
# запасное имя: если сервис подхватит общий railway.toml с «python bot.py», запустится тот же сервер страницы
COPY serve.py bot.py
ENV PYTHONUNBUFFERED=1
CMD ["python", "serve.py"]
