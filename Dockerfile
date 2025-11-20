# Stage 1: Build stage with uv and Python 3.14 (free-threaded)

# Start from a base image (e.g., debian slim)
FROM debian:bookworm-slim AS build

# Install necessary system dependencies for uv (curl, ca-certificates, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates build-essential && rm -rf /var/lib/apt/lists/*

# Install uv by copying the binary from the official uv image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# Set environment variables for uv to manage Python versions
ENV UV_PYTHON_INSTALL_DIR=/usr/local/uv-python
ENV PATH="$UV_PYTHON_INSTALL_DIR/bin:$PATH"

# Install the free-threaded build of Python 3.14 using uv
# The 't' suffix requests the free-threaded (no-GIL) build
RUN uv python install 3.14t

# Set app working directory
WORKDIR /app

# Copy pyproject.toml and uv.lock files for dependency installation
# This layer is cached efficiently if dependencies don't change
COPY pyproject.toml uv.lock ./

# Install project dependencies into a virtual environment
# Use --no-install-project if you are not installing the project as a package, 
# otherwise use `uv sync --locked`
RUN uv sync --locked

# Copy the application source code
COPY . .

# Compile Python files to bytecode for faster startup (optional, but recommended)
ENV UV_COMPILE_BYTECODE=1

# Stage 2: Final runtime stage

# Start from a minimal runtime base image (e.g., distroless or a slim image)
# Note: You might need to adjust the base image based on your app's C-extension dependencies
FROM gcr.io/distroless/cc AS final

# Copy the installed Python environment and virtual environment from the build stage
COPY --from=build /usr/local/uv-python /usr/local/uv-python
COPY --from=build /src /src

# Ensure the Python binaries are on the PATH
ENV PATH="/usr/local/uv-python/bin:/src/.venv/bin:$PATH"

WORKDIR /src

CMD ["uv", "run", "main.py"]
