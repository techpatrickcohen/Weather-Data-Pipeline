FROM python:3.9-slim

# Set working directory
WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

# Set environment variable to point to .streamlit folder for theme config
ENV STREAMLIT_CONFIG_DIR=/app/.streamlit

# Command to run your Streamlit app
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0", "--server.fileWatcherType=auto"]
