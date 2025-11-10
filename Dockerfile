# Use an official Python runtime as a parent image
FROM python:3.12-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Set the working directory in the container
WORKDIR /app

# Copy dependency files
COPY pyproject.toml uv.lock README.md ./

# Install dependencies using uv (creates .venv by default)
RUN uv sync --frozen --no-dev

# Add venv to PATH
ENV PATH="/app/.venv/bin:$PATH"

# Copy the rest of the source code into the container
COPY . .

# Cloud Run expects the container to listen on the port specified by the PORT env var
ENV PORT=8080

# Expose port
EXPOSE 8080

# Set the entry point to the functions framework, specifying the target function name
ENTRYPOINT ["functions-framework", "--target=handle_cloud_event", "--signature-type=cloudevent"]

