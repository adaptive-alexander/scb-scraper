# Base Image
FROM python:3.10.8-slim-bullseye

# Set workdir
WORKDIR /app

# Copy code
COPY . .

# Install dependencies
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Run file
CMD ["python", "scb-update-scheduler.py"]
