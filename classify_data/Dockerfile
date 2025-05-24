# Use official Python image with a compatible version
FROM python:3.12-slim

# Set working directory inside the container
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt --timeout=300

# Copy app source code
COPY main.py gdrive.py /app/

# Copy models folder
COPY models /app/models

# Copy the service account file into the container
COPY peaceprocess-0b61b099c89e.json /app/peaceprocess-0b61b099c89e.json


# Set environment variables if needed
ENV MODEL_PATH="/app/models/all-MiniLM-L6-v2"
ENV CLASSIFIER_ARM_PATH="/app/models/transformer_classifier_arm.joblib"
ENV CLASSIFIER_AZE_PATH="/app/models/transformer_classifier_aze.joblib"
ENV SPACY_MODEL_PATH="/app/models/en_core_web_sm/en_core_web_sm-3.8.0"

# Set the GOOGLE_APPLICATION_CREDENTIALS environment variable to point to the service account file
ENV GOOGLE_APPLICATION_CREDENTIALS="/app/peaceprocess-0b61b099c89e.json"

# Expose port for Cloud Run
EXPOSE 8080

# Command to run your Flask app (adjust if you use something else)
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "main:app"]
