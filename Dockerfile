# Use a lightweight Python base image
FROM python:3.10-slim

# Set working directory in the container
WORKDIR /app

# Copy and install dependencies first for Docker cache efficiency
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code into the container
COPY . .

# Remove any local Streamlit config to avoid TOML parsing errors
RUN rm -rf .streamlit

# Expose the port Streamlit will run on (convention)
EXPOSE 8000

# Start the Streamlit app using shell form to allow $PORT expansion
CMD ["/bin/sh", "-c", "streamlit run app_admin.py --server.port ${PORT:-8000} --server.address 0.0.0.0"]
