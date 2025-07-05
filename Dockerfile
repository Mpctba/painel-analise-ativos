# Use a lightweight Python base image
FROM python:3.10-slim

# Set working directory in the container
WORKDIR /app

# Copy and install dependencies first for Docker cache efficiency
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code into the container
COPY . .

# Expose the port Streamlit will run on (convention)
EXPOSE 8000

# Start the Streamlit app using shell form to allow $PORT expansion
env PORT=${PORT:-8000}
CMD streamlit run app_admin.py --server.port $PORT --server.address 0.0.0.0
