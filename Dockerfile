FROM python:3.12-slim
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
COPY requirements-dashboard.txt .
RUN pip install --no-cache-dir -r requirements-dashboard.txt
COPY dashboard.py .
RUN useradd -m appuser
USER appuser
ENV PORT=8080
EXPOSE 8080
CMD ["sh", "-c", "streamlit run dashboard.py --server.port=${PORT} --server.address=0.0.0.0 --server.headless=true"]
