FROM python:3.8

COPY docker/fastapi_app/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

# Set the working directory
WORKDIR /app

# Copy FastAPI application code into the container
COPY src/fastapi/app.py /app

# Expose the port
EXPOSE 8000

# Run the FastAPI application using uvicorn
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]