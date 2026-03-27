FROM python:3.11

# Install Node.js 20.x
RUN apt-get update \
  && apt-get install -y --no-install-recommends curl \
  && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
  && apt-get install -y nodejs \
  && rm -rf /var/lib/apt/lists/*

# Copy uv from official image
COPY --from=ghcr.io/astral-sh/uv:0.9.26 /uv /uvx /bin/

WORKDIR /app

# Copy dependency files first for caching
COPY frontend/package.json frontend/package-lock.json ./frontend/
COPY backend/pyproject.toml backend/uv.lock ./backend/

# Install dependencies
RUN cd frontend && npm ci \
  && cd ../backend && uv sync --frozen

# Copy project source
COPY . .

# Build frontend for production
RUN cd frontend && npm run build

# Expose single port - Flask serves both API and frontend static files
EXPOSE 5001

# Run Flask backend (serves API + frontend dist)
CMD ["backend/.venv/bin/python", "-m", "flask", "--app", "backend/app", "run", "--host", "0.0.0.0", "--port", "5001"]
