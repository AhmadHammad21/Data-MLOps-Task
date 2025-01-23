# Model Deployment - Flask Application

This project provides a Flask web service that can be used for model deployment. The following instructions will guide you through setting up the environment, installing dependencies, and running the service.

**Note:** 
- The `.env` file is included to allow the application to run smoothly without manually setting up database credentials. This is convenient for this assignment, but **it is a bad practice** to store sensitive information in the `.env` file in production environments.  
- `Redis` is hosted on a free cloud database. 
---

## Prerequisites

- Python 3.12
- Conda or Docker (depending on your preferred method)
- Required libraries listed in `requirements.txt`

---

## Setup Instructions

### 1. **Create a Python Environment**
Create a Conda environment with Python 3.12:

```bash
conda create --name mlops_env python=3.12 -y
conda activate mlops_env
```


### 2. **Install Required Libraries**
Navigate to the Directory and Install the Required Libraries

```bash
cd model_deployment
pip install -r requirements.txt
```

# Running the Web Service

### Method 1: Run Directly Using Python
1. Run the Flask application:
```bash
python app.py
```
2. To test the application, you can run:
```bash
python test.py
```

### Method 2: Run Using Docker
1. Build the Docker Image:
```bash
docker build -t recommend_app .
```
2. Run the Docker Container:
```bash
docker run -p 5000:5000 recommend_app
```
3. Run the test by executing:
```bash
python test.py
```

### Method 3: Run Using Docker Compose
1. Build and Start the Application with Docker Compose:
```bash
docker-compose up --build
```
2. Run the test by executing:
```bash
python test.py
```

Note: You can either test the application by running `python test.py` or by using a `Postman`
to send the requests.
