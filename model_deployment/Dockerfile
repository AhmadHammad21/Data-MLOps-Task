# Use Python base image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Copy application files
COPY . ./

# Install dependencies
RUN pip install -r requirements.txt

# Expose the Flask app's port
EXPOSE 5000 

# Production Usage
ENTRYPOINT ["gunicorn", "--bind=0.0.0.0:5000", "app:app"]


